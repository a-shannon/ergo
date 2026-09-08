package org.ergoplatform.nodeView.wallet

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import org.ergoplatform.modifiers.mempool.UnconfirmedTransaction
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.{FullBlockApplied, SuccessfulTransaction}
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.LocallyGeneratedTransaction
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.mempool.ErgoMemPoolUtils.ProcessingOutcome
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages.CloseWallet
import org.ergoplatform.nodeView.wallet.requests.PaymentRequest
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef}
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestContext, WalletTestOps}
import org.scalatest.concurrent.Eventually
import scorex.db.LDBFactory

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.concurrent.Await
import scala.concurrent.duration._

class WalletOrdinaryRestartSpec extends ErgoCorePropertyTest with WalletTestOps with Eventually {
  import org.ergoplatform.utils.ErgoNodeTestConstants.{defaultTimeout, settings}

  private implicit val timeout: Timeout = defaultTimeout
  private implicit val patience: PatienceConfig = PatienceConfig(20.seconds, 100.millis)

  private class RunningView(override val settings: ErgoSettings) extends NodeViewTestContext {
    override implicit val actorSystem: ActorSystem = ActorSystem()
    override val testProbe: TestProbe = TestProbe()
    val blocks: TestProbe = TestProbe()
    val transactions: TestProbe = TestProbe()
    actorSystem.eventStream.subscribe(blocks.ref, classOf[FullBlockApplied])
    actorSystem.eventStream.subscribe(transactions.ref, classOf[SuccessfulTransaction])
    override val nodeViewHolderRef: ActorRef = ErgoNodeViewRef(settings)
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
    val second = new RunningView(restartSettings)
    try {
      (second.actorSystem eq first.actorSystem) shouldBe false
      second.nodeViewHolderRef should not be first.nodeViewHolderRef
      second.readers.w.walletActor should not be originalWallet

      // Startup alone must restore it: do not resubmit or scan the transaction here.
      val admitted = second.transactions.expectMsgType[SuccessfulTransaction](20.seconds)
      admitted.transaction.id shouldBe transaction.id
      admitted.transaction.transaction.bytes.toVector shouldBe transaction.bytes.toVector
      eventually {
        val current = second.readers
        current.h.bestFullBlockOpt.get.id shouldBe fundingBlock.id
        current.s.stateContext.currentHeight shouldBe fundingBlock.header.height
        current.m.getAll.map(_.id) shouldBe Seq(transaction.id)
        current.m.modifierById(transaction.id).get.bytes.toVector shouldBe transaction.bytes.toVector
        val stored = await(current.w.unconfirmedTransactionsToRestore)
        stored.map(_.id) shouldBe Seq(transaction.id)
        stored.head.bytes.toVector shouldBe transaction.bytes.toVector
      }
    } finally {
      second.close()
    }
    second.actorSystem.whenTerminated.isCompleted shouldBe true

    // Delete only after both systems have closed; retain failed fixture state for diagnosis.
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
