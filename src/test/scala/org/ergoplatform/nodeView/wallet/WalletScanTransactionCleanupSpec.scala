package org.ergoplatform.nodeView.wallet

import java.io.File
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.history.BlockTransactions
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.{CurrentWalletView, RequestCurrentWalletView}
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.WalletTransactionRestorationSupport
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.RestoredTransaction
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{ErgoStateContext, ErgoStateReader}
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages._
import org.ergoplatform.nodeView.wallet.persistence.WalletStorage
import org.ergoplatform.sdk.SecretString
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.ErgoCoreTestConstants.parameters
import org.ergoplatform.utils.ErgoNodeTestConstants.initSettings
import org.ergoplatform.utils.generators.ChainGenerator.genChain
import org.ergoplatform.utils.generators.ErgoNodeTransactionGenerators.validErgoTransactionGenTemplate
import org.ergoplatform.wallet.settings.SecretStorageSettings
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import scorex.db.LDBFactory
import scorex.util.{ModifierId, bytesToId}
import scorex.util.ScorexLogging

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Actor lifecycle tests with controlled scan results; block consensus validation is not exercised. */
class WalletScanTransactionCleanupSpec extends AnyPropSpec with Matchers {
  private case object CheckSnapshotCatchUp
  private case object RestartWalletBeforeView
  private val blockHeight = WalletStorage.UnconfirmedTxLifetimeInBlocks + 10
  private lazy val transactions = Vector.fill(4)(validErgoTransactionGenTemplate(minAssets = 0, maxInputs = 2).sample.get._2)
  private lazy val parentHeader = genChain(1).head.header.copy(height = blockHeight - 1)
  private lazy val block: ErgoFullBlock = {
    val template = genChain(1).head
    val txs = Seq(transactions.head)
    val header = template.header.copy(height = blockHeight, parentId = parentHeader.id,
      transactionsRoot = BlockTransactions.transactionsRoot(txs, template.header.version))
    template.copy(header = header, blockTransactions = BlockTransactions(header.id, header.version, txs),
      extension = template.extension.copy(headerId = header.id), adProofs = template.adProofs.map(_.copy(headerId = header.id)))
  }

  // Confirmed, expired by one block, exactly at the expiry boundary, and unrelated fresh.
  private lazy val records: Vector[(ErgoTransaction, Int)] = transactions.zip(Vector(blockHeight, 9, 10, blockHeight - 1))
  private lazy val allRecords: Map[ModifierId, Int] = records.map { case (tx, height) => tx.id -> height }.toMap
  private lazy val retainedRecords: Map[ModifierId, Int] = records.drop(2).map { case (tx, height) => tx.id -> height }.toMap

  private def persistentRecords(storage: WalletStorage): Map[ModifierId, Int] =
    storage.readUnconfirmedTransactions().map { case (tx, height) => tx.id -> height }.toMap

  private final class ScanService(config: ErgoSettings, val resultStorage: WalletStorage) extends ErgoWalletServiceImpl(config) {
    val originalState = new AtomicReference[ErgoWalletState]()
    val observedState = new AtomicReference[ErgoWalletState]()
    val scans = new AtomicInteger()
    val failed = new AtomicBoolean(false)
    val acknowledgeHeight = new AtomicBoolean(true)
    val thrown = new AtomicBoolean(false)
    val snapshotHelperOnly = new AtomicBoolean(false)
    val returnedResultStorage = new AtomicBoolean(false)
    val error = new IllegalStateException("controlled wallet scan failure")

    override def readWallet(state: ErgoWalletState, testMnemonic: Option[SecretString], testKeysQty: Option[Int],
                            secretStorageSettings: SecretStorageSettings): ErgoWalletState = {
      testMnemonic.foreach(_.erase())
      val original = state.stateContext
      val context = new ErgoStateContext(Seq(parentHeader), None, original.genesisStateDigest,
        original.currentParameters, original.validationSettings, original.votingData)(config.chainSettings)
      Seq(state.storage, resultStorage).foreach { storage =>
        storage.updateStateContext(context).get
        records.foreach { case (tx, height) => storage.addUnconfirmedTransaction(tx, height).get }
      }
      if (state.getWalletHeight < parentHeader.height) {
        state.registry.updateOnBlock(WalletScanLogic.ScanResults(Seq.empty, Seq.empty, Seq.empty),
          parentHeader.id, parentHeader.height).get
      }
      originalState.set(state)
      state
    }

    override def restoreOffChainState(state: ErgoWalletState): ErgoWalletState = state

    override def getWalletBoxes(state: ErgoWalletState, unspentOnly: Boolean,
                                considerUnconfirmed: Boolean): Seq[WalletBox] = {
      observedState.set(state)
      Seq.empty
    }

    override def scanBlockUpdate(state: ErgoWalletState, scanned: ErgoFullBlock, dustLimit: Option[Long]): Try[ErgoWalletState] = {
      require(scanned.id == block.id)
      scans.incrementAndGet()
      persistentRecords(state.storage) shouldBe allRecords
      persistentRecords(resultStorage) shouldBe allRecords
      if (thrown.get()) throw error
      if (failed.get()) Failure(error) else {
        if (acknowledgeHeight.get()) {
          state.registry.updateOnBlock(WalletScanLogic.ScanResults(Seq.empty, Seq.empty, Seq.empty),
            block.id, block.height).get
        }
        returnedResultStorage.set(acknowledgeHeight.get())
        val result = state.copy(storage = resultStorage, error = None)
        result.persistedInputIds.size should be > 0
        Success(result)
      }
    }
  }

  private def withActor(
    test: (TestProbe, ActorRef, ScanService, AtomicBoolean, () => ActorRef) => Unit,
    beforeCurrentView: (TestProbe, ActorRef, ScanService) => Unit = (_, _, _) => (),
    restartBeforeCurrentView: Boolean = false,
    advanceContextAfterStartup: Boolean = true): Unit = {
    val directory = Files.createTempDirectory("wallet-scan-cleanup").toFile
    val config = initSettings.copy(directory = directory.getPath,
      nodeSettings = initSettings.nodeSettings.copy(blocksToKeep = 0))
    require(config.walletSettings.testMnemonic.isDefined, "The actor fixture uses the existing initialized test-wallet setting")
    val resultStorage = new WalletStorage(LDBFactory.createKvDb(new File(directory, "scan-result").getPath), config)
    val service = new ScanService(config, resultStorage)
    val available = new AtomicBoolean(true)
    val history = Proxy.newProxyInstance(classOf[ErgoHistoryReader].getClassLoader, Array(classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
          case "bestFullBlockAt" =>
            if (available.get() && args(0).asInstanceOf[Int] == block.height) Some(block) else None
          case "bestHeaderAtHeight" => args(0).asInstanceOf[Int] match {
            case h if h == parentHeader.height => Some(parentHeader)
            case h if h == block.height => Some(block.header)
            case _ => None
          }
          case "minimalFullBlockHeight" => Int.box(0)
          case "toString" => "wallet scan history fixture"
          case name => throw new IllegalStateException(s"Unexpected history fixture call: $name")
        }
      }).asInstanceOf[ErgoHistoryReader]
    val system = ActorSystem("wallet-scan-cleanup")
    val probe = TestProbe()(system)
    val startup = TestProbe()(system)
    val scanner = TestProbe()(system)
    system.eventStream.subscribe(startup.ref, classOf[RequestCurrentWalletView])
    def startActor(): ActorRef = {
      val actor = system.actorOf(Props(new ErgoWalletActor(config, parameters, service, null, history) {
        override protected[wallet] def createUtxoSnapshotScanner(): ActorRef = scanner.ref
        override def aroundReceive(receive: Receive, message: Any): Unit = message match {
          case RestartWalletBeforeView =>
            // Release fixture-owned native handles before exercising Akka's real restart.
            service.originalState.get().storage.close()
            service.originalState.get().registry.close()
            throw new IllegalStateException("controlled wallet restart before current view")
          case CheckSnapshotCatchUp =>
            service.snapshotHelperOnly.set(true)
            sender() ! scanUtxoSnapshotCatchUpHeight(service.originalState.get(), block.height)
          case _ => super.aroundReceive(receive, message)
        }
      }))
      probe.watch(actor)
      var request = startup.expectMsgType[RequestCurrentWalletView](10.seconds)
      barrier(probe, actor)
      beforeCurrentView(probe, actor, service)
      if (restartBeforeCurrentView) {
        val previousId = request.requestId
        probe.send(actor, RestartWalletBeforeView)
        request = startup.fishForMessage(10.seconds) {
          case next: RequestCurrentWalletView => next.requestId != previousId && next.replyTo == actor
          case _ => false
        }.asInstanceOf[RequestCurrentWalletView]
        barrier(probe, actor).error.get should include("startup canonical alignment is pending")
      }
      val stateContext = service.originalState.get().stateContext
      val stateReader = Proxy.newProxyInstance(classOf[ErgoStateReader].getClassLoader,
        Array(classOf[ErgoStateReader]), new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
            case "stateContext" => stateContext
            case "toString" => "wallet scan state fixture"
            case name => throw new IllegalStateException(s"Unexpected state fixture call: $name")
          }
        }).asInstanceOf[ErgoStateReader]
      probe.send(actor, CurrentWalletView(request.requestId, stateReader, ErgoMemPool.empty(config).getReader, None))
      probe.awaitAssert(barrier(probe, actor).error shouldBe None, 10.seconds, 50.millis)
      if (advanceContextAfterStartup) {
        val updatedContext = new ErgoStateContext(Seq(block.header), None, stateContext.genesisStateDigest,
          stateContext.currentParameters, stateContext.validationSettings, stateContext.votingData)(config.chainSettings)
        Seq(service.originalState.get().storage, resultStorage).foreach(_.updateStateContext(updatedContext).get)
      }
      actor
    }
    var actor = startActor()
    def restartActor(): ActorRef = {
      probe.send(actor, CloseWallet)
      probe.expectTerminated(actor, 5.seconds)
      actor = startActor()
      actor
    }
    try {
      barrier(probe, actor)
      allRecords.size shouldBe 4
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      persistentRecords(resultStorage) shouldBe allRecords
      test(probe, actor, service, available, () => restartActor())
    } finally {
      probe.send(actor, CloseWallet)
      probe.expectTerminated(actor, 5.seconds)
      Await.result(system.terminate(), 10.seconds)
      if (service.returnedResultStorage.get() && !service.snapshotHelperOnly.get()) service.originalState.get().storage.close()
      else resultStorage.close()
      def remove(file: File): Unit = {
        Option(file.listFiles()).foreach(_.foreach(remove))
        if (!file.delete()) file.deleteOnExit()
      }
      remove(directory)
    }
  }

  /** Same-sender status request is a handler barrier and does not expire persisted transactions. */
  private def barrier(probe: TestProbe, actor: akka.actor.ActorRef): WalletStatus = {
    probe.send(actor, GetWalletStatus)
    probe.expectMsgType[WalletStatus](10.seconds)
  }

  private def scan(path: String): Any = path match {
    case "direct" => ScanOnChain(block)
    case "past" => ScanInThePast(block.height, rescan = false)
    case _ => ScanInThePast(block.height, rescan = true)
  }

  for (path <- Seq("direct", "past", "rescan")) {
    property(s"$path scan success removes confirmed and expired records from the returned state only") {
      withActor { (probe, actor, service, _, _) =>
        probe.send(actor, scan(path))
        barrier(probe, actor)
        service.scans.get() shouldBe 1
        persistentRecords(service.resultStorage) shouldBe retainedRecords
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
        probe.send(actor, GetWalletBoxes(unspentOnly = true, considerUnconfirmed = true))
        probe.expectMsg(Seq.empty[WalletBox])
        service.observedState.get().persistedInputIds shouldBe
          records.drop(2).flatMap(_._1.inputs.map(input => bytesToId(input.boxId))).toSet
      }
    }

    property(s"$path scan failure retains every record and a later successful retry performs cleanup") {
      withActor { (probe, actor, service, _, restart) =>
        service.failed.set(true)
        probe.send(actor, scan(path))
        barrier(probe, actor).error.get should include(service.error.getMessage)
        service.scans.get() shouldBe 1
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
        persistentRecords(service.resultStorage) shouldBe allRecords
        service.failed.set(false)
        // Mandatory catch-up failure requires restart in the snapshot-bootstrap composition.
        val retryActor = if (path == "past") restart() else actor
        probe.send(retryActor, scan(path))
        barrier(probe, retryActor)
        service.scans.get() shouldBe 2
        persistentRecords(service.resultStorage) shouldBe retainedRecords
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
      }
    }
  }

  for (rescan <- Seq(false, true)) property(s"missing past block leaves records untouched with rescan=$rescan") {
    withActor { (probe, actor, service, available, _) =>
      available.set(false)
      probe.send(actor, ScanInThePast(block.height, rescan))
      barrier(probe, actor)
      service.scans.get() shouldBe 0
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      persistentRecords(service.resultStorage) shouldBe allRecords
    }
  }

  property("mandatory past scan without height acknowledgement retains every record") {
    withActor { (probe, actor, service, _, _) =>
      service.acknowledgeHeight.set(false)
      probe.send(actor, scan("past"))
      barrier(probe, actor).error.get should include("left the wallet at")
      service.scans.get() shouldBe 1
      persistentRecords(service.resultStorage) shouldBe allRecords
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
    }
  }

  property("a thrown mandatory past scan retains every record behind the recovery guard") {
    withActor { (probe, actor, service, _, _) =>
      service.thrown.set(true)
      probe.send(actor, scan("past"))
      barrier(probe, actor).error.get should include(service.error.getMessage)
      service.scans.get() shouldBe 1
      persistentRecords(service.resultStorage) shouldBe allRecords
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
    }
  }

  property("a deferred direct block does not clean before its missing mandatory predecessor") {
    withActor { (probe, actor, service, available, _) =>
      available.set(false)
      val futureHeader = block.header.copy(height = block.height + 1, parentId = block.id)
      val future = block.copy(header = futureHeader,
        blockTransactions = BlockTransactions(futureHeader.id, futureHeader.version, block.transactions),
        extension = block.extension.copy(headerId = futureHeader.id),
        adProofs = block.adProofs.map(_.copy(headerId = futureHeader.id)))
      probe.send(actor, ScanOnChain(future))
      barrier(probe, actor)
      service.scans.get() shouldBe 0
      persistentRecords(service.resultStorage) shouldBe allRecords
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
    }
  }

  for (outcome <- Seq("success", "height mismatch", "failure", "throw", "missing")) {
    property(s"snapshot catch-up actual-block boundary handles $outcome before cleanup") {
      withActor { (probe, actor, service, available, _) =>
        service.acknowledgeHeight.set(outcome != "height mismatch")
        service.failed.set(outcome == "failure")
        service.thrown.set(outcome == "throw")
        available.set(outcome != "missing")
        probe.send(actor, CheckSnapshotCatchUp)
        val result = probe.expectMsgType[Try[ErgoWalletState]](10.seconds)
        barrier(probe, actor)
        service.scans.get() shouldBe (if (outcome == "missing") 0 else 1)
        result.isSuccess shouldBe (outcome == "success")
        if (outcome == "failure" || outcome == "throw") result.failed.get should be theSameInstanceAs service.error
        if (outcome == "height mismatch") result.failed.get.getMessage should include("left the wallet at")
        if (outcome == "missing") result.failed.get.getMessage should include("unavailable")
        persistentRecords(service.resultStorage) shouldBe (if (outcome == "success") retainedRecords else allRecords)
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
      }
    }
  }

  property("restoration registration waits for the captured view and delivers once despite duplicates") {
    val requester = new AtomicReference[TestProbe]()
    val conflicting = new AtomicReference[TestProbe]()
    val requestId = UUID.randomUUID()
    withActor({ (_, actor, service, _, _) =>
      val reply = requester.get().expectMsgType[WalletTransactionsForRestoration](5.seconds)
      reply.requestId shouldBe requestId
      reply.result.get.map(_.id).toSet shouldBe allRecords.keySet
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      requester.get().send(actor, RegisterWalletTransactionRestoration(requestId))
      requester.get().expectNoMessage(200.millis)
      conflicting.get().expectNoMessage(200.millis)
    }, (probe, actor, _) => {
      requester.set(TestProbe()(probe.system))
      conflicting.set(TestProbe()(probe.system))
      requester.get().send(actor, RegisterWalletTransactionRestoration(requestId))
      requester.get().send(actor, RegisterWalletTransactionRestoration(requestId))
      conflicting.get().send(actor, RegisterWalletTransactionRestoration(UUID.randomUUID()))
      barrier(probe, actor).error.get should include("startup canonical alignment is pending")
      requester.get().expectNoMessage(200.millis)
      conflicting.get().expectNoMessage(200.millis)
    }, advanceContextAfterStartup = false)
  }

  property("an operational wallet serves a new registration with the existing expiry semantics") {
    withActor { (probe, actor, service, _, _) =>
      val requester = TestProbe()(probe.system)
      val id = UUID.randomUUID()
      requester.send(actor, RegisterWalletTransactionRestoration(id))
      val reply = requester.expectMsgType[WalletTransactionsForRestoration](5.seconds)
      reply.requestId shouldBe id
      reply.result.get.map(_.id).toSet shouldBe (allRecords.keySet - transactions(1).id)
      barrier(probe, actor)
      persistentRecords(service.originalState.get().storage) shouldBe (allRecords - transactions(1).id)
      persistentRecords(service.resultStorage) shouldBe allRecords
    }
  }

  property("restoration registration cannot release records from a restart-required quarantine") {
    withActor { (probe, actor, service, _, _) =>
      service.failed.set(true)
      probe.send(actor, scan("past"))
      barrier(probe, actor).error.get should include("restart")
      val requester = TestProbe()(probe.system)
      val id = UUID.randomUUID()
      requester.send(actor, RegisterWalletTransactionRestoration(id))
      requester.send(actor, RegisterWalletTransactionRestoration(id))
      barrier(probe, actor).error.get should include("restart")
      requester.expectNoMessage(300.millis)
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      persistentRecords(service.resultStorage) shouldBe allRecords
    }
  }

  property("an actual wallet actor restart restores its pending holder registration through the new view request") {
    val observed = new AtomicReference[TestProbe]()
    val registrationIds = new java.util.concurrent.ConcurrentLinkedQueue[UUID]()
    val acknowledgedRegistrations = new AtomicInteger(0)
    withActor({ (_, _, _, _, _) =>
      val restored = observed.get().receiveN(4, 5.seconds).map(_.asInstanceOf[RestoredTransaction].unconfirmedTx.id).toSet
      restored shouldBe allRecords.keySet
      import scala.collection.JavaConverters._
      registrationIds.asScala.size should be >= 2
      registrationIds.asScala.toSet.size shouldBe 1
      observed.get().expectNoMessage(200.millis)
    }, (probe, walletActor, _) => {
      observed.set(TestProbe()(probe.system))
      probe.system.actorOf(Props(new Actor with WalletTransactionRestorationSupport with ScorexLogging {
        override protected def currentRestorationWalletActor: ActorRef = walletActor
        override protected def registerWalletTransactionRestoration(id: UUID): Unit = {
          registrationIds.add(id)
          walletActor.tell(RegisterWalletTransactionRestoration(id), self)
          walletActor.tell(GetWalletStatus, self)
        }
        override def preStart(): Unit = {
          context.system.eventStream.subscribe(self, classOf[RequestCurrentWalletView])
          beginWalletTransactionRestoration()
        }
        override def receive: Receive = walletTransactionRestorationResponses.orElse {
          case RequestCurrentWalletView(_, replyTo) => refreshWalletTransactionRestoration(replyTo)
          case _: WalletStatus => acknowledgedRegistrations.incrementAndGet(); ()
          case tx: RestoredTransaction => observed.get().ref ! tx
        }
      }))
      probe.awaitAssert(registrationIds.size() shouldBe 1, 5.seconds, 50.millis)
      probe.awaitAssert(acknowledgedRegistrations.get() shouldBe 1, 5.seconds, 50.millis)
      barrier(probe, walletActor).error.get should include("startup canonical alignment is pending")
      observed.get().expectNoMessage(200.millis)
    }, restartBeforeCurrentView = true, advanceContextAfterStartup = false)
  }
}
