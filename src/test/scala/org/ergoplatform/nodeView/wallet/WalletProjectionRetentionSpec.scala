package org.ergoplatform.nodeView.wallet

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.ChangedMempool
import org.ergoplatform.nodeView.state.ErgoStateContext
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages._
import org.ergoplatform.nodeView.wallet.WalletScanLogic.ScanResults
import org.ergoplatform.nodeView.wallet.persistence.WalletStorage
import org.ergoplatform.nodeView.wallet.scanning.{EqualsScanningPredicate, ScanRequest, ScanWalletInteraction}
import org.ergoplatform.sdk.SecretString
import org.ergoplatform.settings.{Constants, ErgoSettings}
import org.ergoplatform.utils.ErgoCoreTestConstants._
import org.ergoplatform.utils.ErgoNodeTestConstants.initSettings
import org.ergoplatform.utils.MempoolTestHelpers
import org.ergoplatform.utils.generators.ChainGenerator.genChain
import org.ergoplatform.wallet.Constants.{PaymentsScanId, ScanId}
import org.ergoplatform.wallet.boxes.TrackedBox
import org.ergoplatform.wallet.settings.SecretStorageSettings
import org.iq80.leveldb.{DB, Options}
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import scorex.db.{LDBFactory, LDBKVStore}
import scorex.crypto.authds.ADKey
import scorex.util.bytesToId
import sigma.ast.{ByteArrayConstant, ErgoTree}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Try}

/** Ordinary wallet records and controlled storage failures; no consensus validation is exercised. */
class WalletProjectionRetentionSpec extends AnyPropSpec with Matchers with MempoolTestHelpers {
  private val height = WalletStorage.UnconfirmedTxLifetimeInBlocks + 10
  private val script = ErgoTree.fromSigmaBoolean(defaultProver.hdPubKeys.head.key)
  private val funding = ErgoTransaction(IndexedSeq(Input(ADKey @@ Array.fill[Byte](32)(1), emptyProverResult)),
    IndexedSeq(new ErgoBoxCandidate(1000000000L, script, height)))
  private val walletBox = TrackedBox(funding.id, 0, Some(height), None, None, funding.outputs.head, Set(PaymentsScanId))
  private val spending = ErgoTransaction(IndexedSeq(Input(walletBox.box.id, emptyProverResult)),
    IndexedSeq(new ErgoBoxCandidate(900000000L, script, height)))
  private val externalId: ScanId = ScanId @@ 51.toShort
  private val external = ScanRequest("external", EqualsScanningPredicate(ErgoBox.ScriptRegId,
    ByteArrayConstant(Constants.TrueTree.bytes)), Some(ScanWalletInteraction.Off), Some(false)).toScan(externalId).get

  private class ControlledStore(db: DB) extends LDBKVStore(db) {
    @volatile var failInsert = false
    @volatile var failRemove = false
    val error = new IllegalStateException("controlled wallet record failure")
    override def insert(key: Array[Byte], value: Array[Byte]): Try[Unit] =
      if (failInsert) Failure(error) else super.insert(key, value)
    override def remove(keys: Array[Array[Byte]]): Try[Unit] =
      if (failRemove) Failure(error) else super.remove(keys)
  }

  private class Service(config: ErgoSettings, val records: WalletStorage, seed: Seq[(ErgoTransaction, Int)])
    extends ErgoWalletServiceImpl(config) {
    val observed = new AtomicReference[ErgoWalletState]()
    override def readWallet(state: ErgoWalletState, mnemonic: Option[SecretString], keys: Option[Int],
                            secrets: SecretStorageSettings): ErgoWalletState = {
      mnemonic.foreach(_.erase())
      val original = state.stateContext
      val header = genChain(1).head.header.copy(height = height)
      records.updateStateContext(new ErgoStateContext(Seq(header), None, original.genesisStateDigest,
        original.currentParameters, original.validationSettings, original.votingData)(config.chainSettings)).get
      seed.foreach { case (tx, seenAt) => records.addUnconfirmedTransaction(tx, seenAt).get }
      state.registry.updateOnBlock(ScanResults(Seq(walletBox), Seq.empty, Seq.empty), funding.id, height).get
      state.storage.close()
      state.copy(storage = records, walletVars = WalletVars(Some(defaultProver), Seq(external), None)(config))
    }
    override def getWalletBoxes(state: ErgoWalletState, unspentOnly: Boolean,
                                considerUnconfirmed: Boolean): Seq[WalletBox] = {
      observed.set(state)
      super.getWalletBoxes(state, unspentOnly, considerUnconfirmed)
    }
  }

  private class Fixture(val probe: TestProbe, val actor: ActorRef, val service: Service, val store: ControlledStore) {
    // Same-sender service query is a handler barrier, and the real box query evaluates walletFilter.
    def state: ErgoWalletState = {
      probe.send(actor, GetWalletBoxes(unspentOnly = true, considerUnconfirmed = true))
      probe.expectMsgType[Seq[WalletBox]](10.seconds)
      service.observed.get()
    }
    def send(message: Any): ErgoWalletState = { probe.send(actor, message); state }
    def absentView(state: ErgoWalletState): Unit = {
      state.rawOffChainBoxes shouldBe empty
      state.offChainBoxes shouldBe empty
      state.offChainDigest.walletBalance shouldBe walletBox.box.value
    }
  }

  private def withWallet(seed: Seq[(ErgoTransaction, Int)] = Seq.empty)(test: Fixture => Unit): Unit = {
    val directory = Files.createTempDirectory("wallet-projection-retention").toFile
    val config = initSettings.copy(directory = directory.getPath,
      walletSettings = initSettings.walletSettings.copy(secretStorage =
        initSettings.walletSettings.secretStorage.copy(secretDir = new File(directory, "keystore").getPath)))
    val store = new ControlledStore(LDBFactory.factory.open(new File(directory, "records"), new Options().createIfMissing(true)))
    val service = new Service(config, new WalletStorage(store, config), seed)
    val system = ActorSystem("wallet-projection-retention")
    val probe = TestProbe()(system)
    val actor = system.actorOf(Props(new ErgoWalletActor(config, parameters, service, null, null)))
    probe.watch(actor)
    try { val fixture = new Fixture(probe, actor, service, store); fixture.state; test(fixture) }
    finally {
      probe.send(actor, CloseWallet)
      probe.expectTerminated(actor, 10.seconds)
      Await.result(system.terminate(), 10.seconds)
      def remove(file: File): Unit = {
        Option(file.listFiles()).foreach(_.foreach(remove))
        if (!file.delete()) file.deleteOnExit()
      }
      remove(directory)
    }
  }

  property("persisted inputs stay reserved with no reader or an empty mempool without phantom outputs") {
    withWallet() { f =>
      val before = f.state
      before.walletFilter(walletBox) shouldBe true
      val stored = f.send(ScanOffChain(spending))
      stored.walletFilter(walletBox) shouldBe false
      f.absentView(stored)
      stored.storage.readUnconfirmedTransactions().map { case (tx, h) => tx.bytes.toSeq -> h } shouldBe
        Seq(spending.bytes.toSeq -> height)
      val emptyPool = f.send(ChangedMempool(new FakeMempool(Seq.empty)))
      emptyPool.walletFilter(walletBox) shouldBe false
      f.absentView(emptyPool)
      val present = f.send(ChangedMempool(new FakeMempool(Seq(UnconfirmedTransaction(spending, None)))))
      present.walletFilter(walletBox) shouldBe false
      present.persistedInputIds shouldBe Set(bytesToId(walletBox.box.id))
      present.offChainBoxes.map(_.boxId) shouldBe Seq(bytesToId(spending.outputs.head.id))
      present.offChainDigest.walletBalance shouldBe spending.outputs.head.value
      val removed = f.send(ChangedMempool(new FakeMempool(Seq.empty)))
      removed.walletFilter(walletBox) shouldBe false
      f.absentView(removed)
      val forgotten = f.send(ForgetUnconfirmedTransactions(Seq(spending.id)))
      forgotten.walletFilter(walletBox) shouldBe true
      forgotten.persistedInputIds shouldBe empty
      f.absentView(forgotten)
    }
  }

  property("failed record writes retain the prior reservation and projection until acknowledged retry") {
    withWallet() { f =>
      val before = f.state
      before.persistedInputIds shouldBe empty
      f.store.failInsert = true
      val failedAdd = f.send(ScanOffChain(spending))
      failedAdd should be theSameInstanceAs before
      failedAdd.storage.readUnconfirmedTransactions() shouldBe empty
      failedAdd.walletFilter(walletBox) shouldBe true
      f.store.failInsert = false
      val added = f.send(ScanOffChain(spending))
      added.walletFilter(walletBox) shouldBe false
      f.store.failRemove = true
      val failedRemove = f.send(ForgetUnconfirmedTransactions(Seq(spending.id)))
      failedRemove should be theSameInstanceAs added
      failedRemove.storage.readUnconfirmedTransactions().map(_._1.id) shouldBe Seq(spending.id)
      failedRemove.walletFilter(walletBox) shouldBe false
      f.absentView(failedRemove)
      f.store.failRemove = false
      f.send(ForgetUnconfirmedTransactions(Seq(spending.id))).walletFilter(walletBox) shouldBe true
    }
  }

  property("an accepted spend with no wallet output still reserves its wallet input") {
    withWallet() { f =>
      val outgoing = ErgoTransaction(IndexedSeq(Input(walletBox.box.id, emptyProverResult)),
        IndexedSeq(new ErgoBoxCandidate(900000000L, Constants.FalseTree, height)))
      val stored = f.send(ScanOffChain(outgoing))
      stored.walletFilter(walletBox) shouldBe false
      stored.storage.readUnconfirmedTransactions().map(_._1.id) shouldBe Seq(outgoing.id)
      f.absentView(stored)
    }
  }

  property("startup reservations survive failed expiry cleanup and release after successful expiry") {
    withWallet(Seq(spending -> 9)) { f =>
      f.state.walletFilter(walletBox) shouldBe false
      f.store.failRemove = true
      f.probe.send(f.actor, ReadUnconfirmedTransactions)
      f.probe.expectMsg(Seq.empty[ErgoTransaction])
      f.state.walletFilter(walletBox) shouldBe false
      f.store.failRemove = false
      f.probe.send(f.actor, ReadUnconfirmedTransactions)
      f.probe.expectMsg(Seq.empty[ErgoTransaction])
      val expired = f.state
      expired.walletFilter(walletBox) shouldBe true
      expired.storage.readUnconfirmedTransactions() shouldBe empty
      f.absentView(expired)
    }
  }

  property("external-scan records preserve per-scan removeOffchain behavior and irrelevant transactions are not stored") {
    withWallet() { f =>
      val externalTx = ErgoTransaction(IndexedSeq(Input(ADKey @@ Array.fill[Byte](32)(3), emptyProverResult)),
        IndexedSeq(new ErgoBoxCandidate(100000000L, Constants.TrueTree, height)))
      val child = ErgoTransaction(IndexedSeq(Input(externalTx.outputs.head.id, emptyProverResult)),
        IndexedSeq(new ErgoBoxCandidate(90000000L, Constants.FalseTree, height)))
      f.send(ScanOffChain(externalTx)).storage.readUnconfirmedTransactions().map(_._1.id) shouldBe Seq(externalTx.id)
      // The durable parent identifies the spent scan output even before a mempool event arrives.
      val chained = f.send(ScanOffChain(child))
      chained.storage.readUnconfirmedTransactions().map(_._1.id).toSet shouldBe Set(externalTx.id, child.id)
      f.absentView(chained)
      val pool = f.send(ChangedMempool(new FakeMempool(Seq(UnconfirmedTransaction(externalTx, None),
        UnconfirmedTransaction(child, None)))))
      f.service.getScanUnspentBoxes(pool, externalId, true, 0, Int.MaxValue).map(_.trackedBox.boxId) shouldBe
        Seq(bytesToId(externalTx.outputs.head.id))
      pool.offChainDigest.walletBalance shouldBe walletBox.box.value
      pool.offChainBoxes shouldBe empty
      val unrelated = ErgoTransaction(IndexedSeq(Input(ADKey @@ Array.fill[Byte](32)(4), emptyProverResult)),
        IndexedSeq(new ErgoBoxCandidate(1L, Constants.FalseTree, height)))
      f.send(ScanOffChain(unrelated)).storage.readUnconfirmedTransactions().map(_._1.id).toSet shouldBe
        Set(externalTx.id, child.id)
    }
  }
}
