package org.ergoplatform.nodeView.history

import org.ergoplatform.ErgoBox
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.history.ErgoHistoryUtils._
import org.ergoplatform.nodeView.history.storage.modifierprocessors.UtxoSetSnapshotProcessor
import org.ergoplatform.nodeView.state.{BoxHolder, StateType, UtxoState}
import org.ergoplatform.settings.{Algos, Constants, ErgoSettings}
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.core.VersionTag
import org.ergoplatform.serialization.{ManifestSerializer, SubtreeSerializer}
import scorex.crypto.authds.avltree.batch.VersionedLDBAVLStorage
import scorex.db.LDBVersionedStore
import scorex.util.{ModifierId, bytesToId}
import sigma.Colls

import scala.util.Random

class UtxoSetSnapshotProcessorSpecification extends ErgoCorePropertyTest {
  import org.ergoplatform.utils.HistoryTestHelpers.generateHistory
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants._
  import org.ergoplatform.utils.generators.ChainGenerator._
  import org.ergoplatform.utils.generators.ErgoNodeTransactionGenerators._
  import org.ergoplatform.utils.generators.ValidBlocksGenerators._

  val s = settings

  val epochLength = 20

  val utxoSetSnapshotProcessor = new UtxoSetSnapshotProcessor {
    var minimalFullBlockHeightVar = GenesisHeight
    override protected val settings: ErgoSettings = s.copy(chainSettings =
      s.chainSettings.copy(voting = s.chainSettings.voting.copy(votingLength = epochLength)))
    override protected val historyStorage: HistoryStorage = HistoryStorage(settings)
    override def readMinimalFullBlockHeight() = minimalFullBlockHeightVar
    override def writeMinimalFullBlockHeight(height: Int): Unit = {
      minimalFullBlockHeightVar = height
    }
  }

  var history = generateHistory(
    verifyTransactions = true,
    StateType.Utxo,
    PoPoWBootstrap = false,
    blocksToKeep = -1,
    epochLength = epochLength,
    useLastEpochs = 2,
    initialDiffOpt = None)

  val chain = genHeaderChain(epochLength + 1, history, diffBitsOpt = None, useRealTs = false)
  history = applyHeaderChain(history, chain)

  property("registerManifestToDownload + getUtxoSetSnapshotDownloadPlan + getChunkIdsToDownload") {
    val bh     = boxesHolderGenOfSize(32 * 1024).sample.get
    val us     = createUtxoState(bh, parameters)

    val snapshotHeight = epochLength - 1
    val serializer = ManifestSerializer.defaultSerializer

    us.dumpSnapshot(snapshotHeight, us.rootDigest.dropRight(1))
    val manifestId = us.snapshotsDb.readSnapshotsInfo.availableManifests.apply(snapshotHeight)
    val manifestBytes = us.snapshotsDb.readManifestBytes(manifestId).get
    val manifest = serializer.parseBytes(manifestBytes)
    val subtreeIds = manifest.subtreesIds
    val subtreeIdsEncoded = subtreeIds.map(id => ModifierId @@ Algos.encode(id))

    subtreeIds.foreach {sid =>
      val subtreeBytes = us.snapshotsDb.readSubtreeBytes(sid).get
      val subtree = SubtreeSerializer.parseBytes(subtreeBytes)
      subtree.verify(sid) shouldBe true
    }

    val blockId = ModifierId @@ Algos.encode(Array.fill(32)(Random.nextInt(100).toByte))
    utxoSetSnapshotProcessor.registerManifestToDownload(manifest, snapshotHeight, Seq.empty)
    val dp = utxoSetSnapshotProcessor.utxoSetSnapshotDownloadPlan().get
    dp.snapshotHeight shouldBe snapshotHeight
    val expected = dp.expectedChunkIds.map(id => ModifierId @@ Algos.encode(id))
    expected shouldBe subtreeIdsEncoded
    val toDownload = utxoSetSnapshotProcessor.getChunkIdsToDownload(expected.size).map(id => ModifierId @@ Algos.encode(id))
    toDownload shouldBe expected

    subtreeIds.foreach { subtreeId =>
      val subtreeBytes = us.snapshotsDb.readSubtreeBytes(subtreeId).get
      utxoSetSnapshotProcessor.registerDownloadedChunk(subtreeId, subtreeBytes)
    }
    val s = utxoSetSnapshotProcessor.downloadedChunksIterator().map(s => ModifierId @@ Algos.encode(s.id)).toSeq
    s shouldBe subtreeIdsEncoded

    val dir = createTempDir
    val store = new LDBVersionedStore(dir, initialKeepVersions = 100)
    val restoredProver = utxoSetSnapshotProcessor.createPersistentProver(store, history, snapshotHeight, blockId).get
    bh.sortedBoxes.foreach { box =>
      restoredProver.unauthenticatedLookup(box.id).isDefined shouldBe true
    }
    restoredProver.checkTree(postProof = false)
    val restoredState = new UtxoState(restoredProver, version = VersionTag @@@ blockId, store, settings)
    restoredState.stateContext.currentHeight shouldBe (epochLength - 1)
    bh.sortedBoxes.foreach { box =>
      restoredState.boxById(box.id).isDefined shouldBe true
    }
  }

  property("snapshot restore into a reopened zero-retention store keeps its version") {
    val txId = bytesToId(Array.fill[Byte](32)(17))
    val boxes = (0 until 32 * 1024).map { index =>
      new ErgoBox(
        1000000L,
        Constants.TrueTree,
        Colls.emptyColl,
        Map.empty,
        txId,
        index.toShort,
        creationHeight = 0
      )
    }
    val boxHolder = BoxHolder(boxes)
    val sourceSettings = s.copy(directory = createTempDir.getAbsolutePath)
    val sourceState = UtxoState.fromBoxHolder(
      boxHolder, None, createTempDir, sourceSettings, parameters)
    val snapshotHeight = epochLength - 1
    val blockId = ModifierId @@ Algos.encode(Array.fill[Byte](32)(42))

    val manifest = try {
      sourceState.dumpSnapshot(snapshotHeight, sourceState.rootDigest.dropRight(1)).get
      val manifestId = sourceState.snapshotsDb.readSnapshotsInfo
        .availableManifests(snapshotHeight)
      val bytes = sourceState.snapshotsDb.readManifestBytes(manifestId).get
      val parsed = ManifestSerializer.defaultSerializer.parseBytes(bytes)
      val subtreeIds = parsed.subtreesIds
      withClue("SNAPSHOT_ZERO_RETENTION_SETUP: ") {
        subtreeIds.nonEmpty shouldBe true
      }
      utxoSetSnapshotProcessor.registerManifestToDownload(
        parsed, snapshotHeight, Seq.empty)
      val requested = utxoSetSnapshotProcessor.getChunkIdsToDownload(subtreeIds.size)
      requested.map(_.toSeq) shouldBe subtreeIds.map(_.toSeq)
      subtreeIds.foreach { id =>
        val chunk = sourceState.snapshotsDb.readSubtreeBytes(id).get
        utxoSetSnapshotProcessor.registerDownloadedChunk(id, chunk)
      }
      parsed
    } finally sourceState.closeStorage()

    val digest = VersionedLDBAVLStorage.digest(manifest.id, manifest.rootHeight)
    val oldVersion = Array.fill[Byte](32)(1)
    val nextVersion = Array.fill[Byte](32)(2)
    val seedKey = Array.fill[Byte](48)(17)

    def restoredVersionAfterReopen(dir: java.io.File,
                                   seedUndo: Boolean): Option[Seq[Byte]] = {
      if (seedUndo) {
        val seeded = new LDBVersionedStore(dir, initialKeepVersions = 2)
        try {
          seeded.update(oldVersion, Nil, Seq(seedKey -> Array[Byte](11))).get
          seeded.update(nextVersion, Nil, Seq(seedKey -> Array[Byte](12))).get
          seeded.versionIdExists(oldVersion) shouldBe true
          seeded.lastVersionID.map(_.toSeq) shouldBe Some(nextVersion.toSeq)
        } finally seeded.close()
      }

      val store = new LDBVersionedStore(dir, initialKeepVersions = 0)
      try {
        val before = store.lastVersionID.map(_.toSeq)
        if (seedUndo) before shouldBe Some(nextVersion.toSeq)
        else before shouldBe None

        val prover = utxoSetSnapshotProcessor.createPersistentProver(
          store, history, snapshotHeight, blockId).get
        withClue("SNAPSHOT_ZERO_RETENTION_RESTORE_SETUP: ") {
          prover.digest.toSeq shouldBe digest.toSeq
          val firstBox = boxHolder.sortedBoxes.head
          prover.unauthenticatedLookup(firstBox.id).isDefined shouldBe true
          store.lastVersionID.map(_.toSeq) shouldBe Some(digest.toSeq)
        }
      } finally store.close()

      val reopened = new LDBVersionedStore(dir, initialKeepVersions = 0)
      try reopened.lastVersionID.map(_.toSeq)
      finally reopened.close()
    }

    val cleanVersion = restoredVersionAfterReopen(createTempDir, seedUndo = false)
    withClue("SNAPSHOT_ZERO_RETENTION_CLEAN_CONTROL: ") {
      cleanVersion shouldBe Some(digest.toSeq)
    }
    val previouslyVersioned = restoredVersionAfterReopen(createTempDir, seedUndo = true)
    withClue("SNAPSHOT_ZERO_RETENTION_REOPEN_VERSION: ") {
      previouslyVersioned shouldBe Some(digest.toSeq)
    }
  }

}
