package org.ergoplatform.nodeView.wallet

import java.io.File
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.history.BlockTransactions
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.state.ErgoStateContext
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
import scorex.util.ModifierId

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Actor lifecycle tests with controlled scan results; block consensus validation is not exercised. */
class WalletScanTransactionCleanupSpec extends AnyPropSpec with Matchers {
  private val blockHeight = WalletStorage.UnconfirmedTxLifetimeInBlocks + 10
  private lazy val transactions = Vector.fill(4)(validErgoTransactionGenTemplate(minAssets = 0, maxInputs = 2).sample.get._2)
  private lazy val block: ErgoFullBlock = {
    val template = genChain(1).head
    val txs = Seq(transactions.head)
    val header = template.header.copy(height = blockHeight,
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
    val scans = new AtomicInteger()
    val failed = new AtomicBoolean(false)
    val returnedResultStorage = new AtomicBoolean(false)
    val error = new IllegalStateException("controlled wallet scan failure")

    override def readWallet(state: ErgoWalletState, testMnemonic: Option[SecretString], testKeysQty: Option[Int],
                            secretStorageSettings: SecretStorageSettings): ErgoWalletState = {
      testMnemonic.foreach(_.erase())
      val original = state.stateContext
      val context = new ErgoStateContext(Seq(block.header), None, original.genesisStateDigest,
        original.currentParameters, original.validationSettings, original.votingData)(config.chainSettings)
      Seq(state.storage, resultStorage).foreach { storage =>
        storage.updateStateContext(context).get
        records.foreach { case (tx, height) => storage.addUnconfirmedTransaction(tx, height).get }
      }
      originalState.set(state)
      state
    }

    override def restoreOffChainState(state: ErgoWalletState): ErgoWalletState = state

    override def scanBlockUpdate(state: ErgoWalletState, scanned: ErgoFullBlock, dustLimit: Option[Long]): Try[ErgoWalletState] = {
      require(scanned.id == block.id)
      scans.incrementAndGet()
      if (failed.get()) Failure(error) else {
        returnedResultStorage.set(true)
        Success(state.copy(storage = resultStorage, error = None))
      }
    }
  }

  private def withActor(test: (TestProbe, akka.actor.ActorRef, ScanService, AtomicBoolean) => Unit): Unit = {
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
          case "toString" => "wallet scan history fixture"
          case name => throw new IllegalStateException(s"Unexpected history fixture call: $name")
        }
      }).asInstanceOf[ErgoHistoryReader]
    val system = ActorSystem("wallet-scan-cleanup")
    val probe = TestProbe()(system)
    val actor = system.actorOf(Props(new ErgoWalletActor(config, parameters, service, null, history)))
    probe.watch(actor)
    try {
      barrier(probe, actor)
      allRecords.size shouldBe 4
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      persistentRecords(resultStorage) shouldBe allRecords
      test(probe, actor, service, available)
    } finally {
      probe.send(actor, CloseWallet)
      probe.expectTerminated(actor, 5.seconds)
      Await.result(system.terminate(), 10.seconds)
      if (service.returnedResultStorage.get()) service.originalState.get().storage.close()
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
      withActor { (probe, actor, service, _) =>
        probe.send(actor, scan(path))
        barrier(probe, actor)
        service.scans.get() shouldBe 1
        persistentRecords(service.resultStorage) shouldBe retainedRecords
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
      }
    }

    property(s"$path scan failure retains every record and a later successful retry performs cleanup") {
      withActor { (probe, actor, service, _) =>
        service.failed.set(true)
        probe.send(actor, scan(path))
        barrier(probe, actor).error.get should include(service.error.getMessage)
        service.scans.get() shouldBe 1
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
        persistentRecords(service.resultStorage) shouldBe allRecords
        service.failed.set(false)
        probe.send(actor, scan(path))
        barrier(probe, actor)
        service.scans.get() shouldBe 2
        persistentRecords(service.resultStorage) shouldBe retainedRecords
        persistentRecords(service.originalState.get().storage) shouldBe allRecords
      }
    }
  }

  for (rescan <- Seq(false, true)) property(s"missing past block leaves records untouched with rescan=$rescan") {
    withActor { (probe, actor, service, available) =>
      available.set(false)
      probe.send(actor, ScanInThePast(block.height, rescan))
      barrier(probe, actor)
      service.scans.get() shouldBe 0
      persistentRecords(service.originalState.get().storage) shouldBe allRecords
      persistentRecords(service.resultStorage) shouldBe allRecords
    }
  }
}
