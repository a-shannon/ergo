package org.ergoplatform.nodeView.wallet

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import org.ergoplatform.modifiers.mempool.UnconfirmedTransaction
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.{DeclinedTransaction, FullBlockApplied, SuccessfulTransaction}
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.LocallyGeneratedTransaction
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.mempool.ErgoMemPoolUtils.ProcessingOutcome
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages.CloseWallet
import org.ergoplatform.nodeView.wallet.persistence.WalletStorage
import org.ergoplatform.nodeView.wallet.requests.PaymentRequest
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef, UtxoNodeViewHolder}
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestContext, WalletTestOps}
import org.scalatest.concurrent.Eventually
import scorex.db.LDBFactory
import scorex.util.ModifierId

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.concurrent.Await
import scala.concurrent.duration._

class WalletOrdinaryRestartSpec extends ErgoCorePropertyTest with WalletTestOps with Eventually {
  import org.ergoplatform.utils.ErgoNodeTestConstants.{defaultTimeout, settings}

  private implicit val timeout: Timeout = defaultTimeout
  private implicit val patience: PatienceConfig = PatienceConfig(20.seconds, 100.millis)

  private sealed trait RestartMode
  private case object Ordinary extends RestartMode
  private case object HigherFee extends RestartMode
  private case object OrderingLoss extends RestartMode
  private case object InvalidatedOutcome extends RestartMode
  private case object ReadAdmissionAttempts

  // Supply an already classified outcome to the real restoration handler.
  // This isolates caller policy without constructing conflicting or invalid transactions.
  private class ClassifiedNodeViewHolder(settings: ErgoSettings,
                                       outcome: ProcessingOutcome,
                                       observed: ActorRef) extends UtxoNodeViewHolder(settings) {
    private var attempts = Vector.empty[ModifierId]

    override protected def txModify(tx: UnconfirmedTransaction): ProcessingOutcome = {
      attempts :+= tx.id
      observed ! tx.id
      outcome
    }

    override def receive: Receive = {
      case ReadAdmissionAttempts => sender() ! attempts
      case message => super.receive(message)
    }
  }

  private class RunningView(override val settings: ErgoSettings,
                            classifiedOutcome: Option[ProcessingOutcome] = None) extends NodeViewTestContext {
    override implicit val actorSystem: ActorSystem = ActorSystem()
    override val testProbe: TestProbe = TestProbe()
    val blocks: TestProbe = TestProbe()
    val transactions: TestProbe = TestProbe()
    val admissions: TestProbe = TestProbe()
    actorSystem.eventStream.subscribe(blocks.ref, classOf[FullBlockApplied])
    actorSystem.eventStream.subscribe(transactions.ref, classOf[SuccessfulTransaction])
    actorSystem.eventStream.subscribe(transactions.ref, classOf[DeclinedTransaction])
    override val nodeViewHolderRef: ActorRef = classifiedOutcome match {
      case Some(outcome) => actorSystem.actorOf(
        Props(new ClassifiedNodeViewHolder(settings, outcome, admissions.ref))
          .withDispatcher("critical-dispatcher"))
      case None => ErgoNodeViewRef(settings)
    }
    private val readersRef: ActorRef = ErgoReadersHolderRef(nodeViewHolderRef)

    def readers: Readers = await((readersRef ? GetReaders).mapTo[Readers])

    def close(): Unit = {
      try {
        val walletRef = readers.w.walletActor
        val stopped = TestProbe()
        stopped.watch(walletRef)
        walletRef ! CloseWallet
        stopped.expectTerminated(walletRef, 20.seconds)
      } finally {
        Await.result(actorSystem.terminate(), 30.seconds)
        // This revision leaves snapshot storage outside actor shutdown.
        // The one-block fixture starts no asynchronous snapshot work.
        LDBFactory.createKvDb(s"${settings.directory}/snapshots").close()
      }
    }
  }

  property("restore an admitted wallet transaction after closing and reopening its stores") {
    exerciseRestoration(Ordinary)
  }

  property("retain a wallet transaction through temporary fee policy and restore it on a later restart") {
    exerciseRestoration(HigherFee)
  }

  property("retain a stored transaction after a classified ordering loss and restore it on a later restart") {
    exerciseRestoration(OrderingLoss)
  }

  property("preserve restoration cleanup for a classified invalidated outcome") {
    exerciseRestoration(InvalidatedOutcome)
  }

  private def exerciseRestoration(mode: RestartMode): Unit = {
    val deferred = mode == HigherFee || mode == OrderingLoss
    val root = Files.createTempDirectory("wallet-ordinary-restart-")
    val restartSettings = settings.copy(
      directory = root.toString,
      walletSettings = settings.walletSettings.copy(
        secretStorage = settings.walletSettings.secretStorage.copy(
          secretDir = root.resolve("wallet/keystore").toString
        )
      )
    )

    val first = new RunningView(restartSettings)
    val (transaction, fundingBlock, originalWallet) = try {
      val initial = first.readers
      initial.m.getAll shouldBe empty
      await(initial.w.unconfirmedTransactionsToRestore) shouldBe empty
      val address = await(initial.w.publicKeys(0, Int.MaxValue)).head
      val funding = makeGenesisBlock(address.pubkey)(first)
      applyBlock(funding)(first).get
      first.blocks.expectMsgType[FullBlockApplied](20.seconds).header.id shouldBe funding.id

      val funded = eventually {
        val current = first.readers
        current.h.bestFullBlockOpt.get.id shouldBe funding.id
        current.s.stateContext.currentHeight shouldBe funding.header.height
        val balance = await(current.w.confirmedBalances)
        balance.height shouldBe funding.header.height
        balance.walletBalance should be > 0L
        (current, balance.walletBalance)
      }
      val request = PaymentRequest(address, funded._2 / 2, Array.empty, Map.empty)
      val tx = await(funded._1.w.generateTransaction(Seq(request))).get
      first.testProbe.send(first.nodeViewHolderRef,
        LocallyGeneratedTransaction(UnconfirmedTransaction(tx, None)))
      first.testProbe.expectMsgType[ProcessingOutcome.Accepted](20.seconds).tx.id shouldBe tx.id
      first.transactions.expectMsgType[SuccessfulTransaction](20.seconds).transaction.id shouldBe tx.id

      eventually {
        val current = first.readers
        current.m.getAll.map(_.id) shouldBe Seq(tx.id)
        current.m.modifierById(tx.id).get.bytes.toVector shouldBe tx.bytes.toVector
        val stored = await(current.w.unconfirmedTransactionsToRestore)
        stored.map(_.id) shouldBe Seq(tx.id)
        stored.head.bytes.toVector shouldBe tx.bytes.toVector
      }
      (tx, funding, initial.w.walletActor)
    } finally {
      first.close()
    }

    first.actorSystem.whenTerminated.isCompleted shouldBe true

    def readStoredRecords(): Vector[(ModifierId, Vector[Byte], Int)] = {
      val storage = WalletStorage.readOrCreate(restartSettings)
      try {
        storage.readUnconfirmedTransactions().map { case (tx, height) =>
          (tx.id, tx.bytes.toVector, height)
        }.toVector
      } finally {
        storage.close()
      }
    }

    val originalRecords = readStoredRecords()
    originalRecords shouldBe Vector((transaction.id, transaction.bytes.toVector, fundingBlock.header.height))

    def awaitRestored(view: RunningView): Unit = {
      // Startup alone must restore it: do not resubmit or scan the transaction here.
      val admitted = view.transactions.expectMsgType[SuccessfulTransaction](20.seconds)
      admitted.transaction.id shouldBe transaction.id
      admitted.transaction.transaction.bytes.toVector shouldBe transaction.bytes.toVector
      getCurrentView(view)
      eventually {
        val current = view.readers
        current.h.bestFullBlockOpt.get.id shouldBe fundingBlock.id
        current.s.stateContext.currentHeight shouldBe fundingBlock.header.height
        current.m.getAll.map(_.id) shouldBe Seq(transaction.id)
        current.m.modifierById(transaction.id).get.bytes.toVector shouldBe transaction.bytes.toVector
        val stored = await(current.w.unconfirmedTransactionsToRestore)
        stored.map(_.id) shouldBe Seq(transaction.id)
        stored.head.bytes.toVector shouldBe transaction.bytes.toVector
      }
    }

    val secondSettings = if (mode == HigherFee) {
      restartSettings.nodeSettings.minimalFeeAmount shouldBe 0L
      restartSettings.copy(nodeSettings = restartSettings.nodeSettings.copy(minimalFeeAmount = 1L))
    } else {
      restartSettings
    }
    val classifiedOutcome = mode match {
      case OrderingLoss => Some(new ProcessingOutcome.DoubleSpendingLoser(Set.empty, System.currentTimeMillis()))
      case InvalidatedOutcome => Some(new ProcessingOutcome.Invalidated(
        new Exception("classified restoration outcome"), System.currentTimeMillis()))
      case _ => None
    }
    val second = new RunningView(secondSettings, classifiedOutcome)
    try {
      (second.actorSystem eq first.actorSystem) shouldBe false
      second.nodeViewHolderRef should not be first.nodeViewHolderRef
      second.readers.w.walletActor should not be originalWallet

      if (mode != Ordinary) {
        if (classifiedOutcome.isDefined) {
          second.admissions.expectMsg(20.seconds, transaction.id)
        } else {
          second.transactions.expectMsgType[DeclinedTransaction](20.seconds).transaction.id shouldBe transaction.id
        }
        // The outcome notification precedes the restoration handler's storage decision.
        // Ask that same holder before inspecting its wallet or closing any stores.
        getCurrentView(second)
        if (classifiedOutcome.isDefined) {
          await((second.nodeViewHolderRef ? ReadAdmissionAttempts).mapTo[Vector[ModifierId]]) shouldBe
            Vector(transaction.id)
        }
        eventually {
          val current = second.readers
          current.m.getAll shouldBe empty
          val stored = await(current.w.unconfirmedTransactionsToRestore)
          if (deferred) {
            stored.map(_.id) shouldBe Seq(transaction.id)
            stored.head.bytes.toVector shouldBe transaction.bytes.toVector
          } else {
            stored shouldBe empty
          }
        }
      } else {
        awaitRestored(second)
      }
    } finally {
      second.close()
    }
    second.actorSystem.whenTerminated.isCompleted shouldBe true
    readStoredRecords() shouldBe (if (mode == InvalidatedOutcome) Vector.empty else originalRecords)

    if (deferred) {
      val third = new RunningView(restartSettings)
      try {
        third.nodeViewHolderRef should not be second.nodeViewHolderRef
        third.readers.w.walletActor should not be originalWallet
        awaitRestored(third)
      } finally {
        third.close()
      }
      third.actorSystem.whenTerminated.isCompleted shouldBe true
      readStoredRecords() shouldBe originalRecords
    }

    // Delete only after all systems have closed; retain failed fixture state for diagnosis.
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(path: Path, attributes: BasicFileAttributes): FileVisitResult = {
        Files.delete(path)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(path: Path, error: IOException): FileVisitResult = {
        if (error != null) throw error
        Files.delete(path)
        FileVisitResult.CONTINUE
      }
    })
    Files.exists(root) shouldBe false
  }
}
