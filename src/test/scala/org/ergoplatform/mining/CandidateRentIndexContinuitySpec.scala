package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.ByteBuffer
import scorex.util.idToBytes

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.BlockSection
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, ExtraIndexer, StorageRentBox}
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.history.storage.modifierprocessors.FullBlockProcessor
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{BoxHolder, ErgoStateContext, UtxoState, VotingData}
import org.ergoplatform.settings.Constants
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.utils.TestFileUtils
import sigmastate.helpers.TestingHelpers._

import org.ergoplatform.utils.generators.ErgoCoreTransactionGenerators._

/** A once-current extra index must not authorize a later storage-rent scan after it lags. */
class CandidateRentIndexContinuitySpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  property("a later index lag stops rent scanning after an earlier synced candidate") {
    val height = 3 * Constants.StoragePeriod
    val witnessBox = testBox(10000000000L, Constants.TrueTree,
      height - 2, Seq.empty, Map.empty)
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))

    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(witnessBox)), None,
      createTempDir, fixtureSettings, parameters)
    val block = invalidErgoFullBlockGen.sample.get
    val selectedHeader = block.header.copy(height = height - 1)
    val sideHeader = selectedHeader.copy(timestamp = selectedHeader.timestamp + 1)
    val ancestorHeader = selectedHeader.copy(height = height - 4,
      timestamp = selectedHeader.timestamp + 2)
    var parentHeader = selectedHeader
    var parentBlock = block.copy(parentHeader)
    var context = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = context
      override def emissionBoxOpt: Option[ErgoBox] = None
    }

    val storage = HistoryStorage(fixtureSettings)
    val indexedHeight = parentHeader.height
    storage.insert(Array(
      FullBlockProcessor.chainStatusKey(selectedHeader.id) -> FullBlockProcessor.BestChainMarker,
      FullBlockProcessor.chainStatusKey(ancestorHeader.id) -> FullBlockProcessor.BestChainMarker
    ), Array[BlockSection](selectedHeader, ancestorHeader)).get
    storage.insertExtra(Array(
      ExtraIndexer.IndexedHeightKey -> ByteBuffer.allocate(4).putInt(indexedHeight).array,
      ExtraIndexer.IndexedHeaderIdKey -> idToBytes(selectedHeader.id)
    ), Array.empty[ExtraIndex])
    var fullHeight = indexedHeight
    var rentScans = 0
    var switchCheckpointOnNextFullHeightRead = false
    val history = Proxy.newProxyInstance(
      classOf[ErgoHistoryReader].getClassLoader,
      Array[Class[_]](classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "historyStorage" => storage
            case "bestFullBlockOpt" => Some(parentBlock)
            case "fullBlockHeight" =>
              val observed = fullHeight
              if (switchCheckpointOnNextFullHeightRead) {
                switchCheckpointOnNextFullHeightRead = false
                storage.insertExtraTry(Array(
                  ExtraIndexer.IndexedHeightKey ->
                    ByteBuffer.allocate(4).putInt(ancestorHeader.height).array,
                  ExtraIndexer.IndexedHeaderIdKey -> idToBytes(ancestorHeader.id)
                ), Array.empty).get
              }
              Int.box(observed)
            case "storageRentBoxesAtOrBefore" =>
              rentScans += 1
              Array.empty[StorageRentBox]
            case "typedModifierById" =>
              if (args(0) == selectedHeader.id) Some(selectedHeader)
              else if (args(0) == sideHeader.id) Some(sideHeader)
              else if (args(0) == ancestorHeader.id) Some(ancestorHeader)
              else None
            case "requiredDifficultyAfter" => parentHeader.requiredDifficulty
            case other => throw new UnsupportedOperationException(s"unexpected history read: $other")
          }
      }
    ).asInstanceOf[ErgoHistoryReader]

    try {
      val pool = ErgoMemPool.empty(fixtureSettings)
      val witness = ErgoTransaction(
        IndexedSeq(Input(witnessBox.id, emptyProverResult)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(witnessBox.value, Constants.TrueTree, height)))

      val disabledSettings = fixtureSettings.copy(nodeSettings =
        fixtureSettings.nodeSettings.copy(storageRentCollection = false))
      storage.insertExtraTry(Array(ExtraIndexer.IndexedHeightKey -> Array[Byte](1)),
        Array.empty).get
      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, disabledSettings).map(_.isSuccess) shouldBe Some(true)
      rentScans shouldBe 0
      storage.insertExtraTry(Array(ExtraIndexer.IndexedHeightKey ->
        ByteBuffer.allocate(4).putInt(indexedHeight).array), Array.empty).get

      storage.insertExtraTry(Array(ExtraIndexer.RollbackToKey ->
        ByteBuffer.allocate(4).putInt(indexedHeight - 1).array), Array.empty).get
      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      withClue("the extra index rollback is in progress: ") {
        rentScans shouldBe 0
      }
      storage.insertExtraTry(Array(ExtraIndexer.RollbackToKey ->
        ByteBuffer.allocate(4).putInt(0).array), Array.empty).get

      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      rentScans shouldBe 1

      parentHeader = parentHeader.copy(height = indexedHeight + 1)
      parentBlock = block.copy(parentHeader)
      context = new ErgoStateContext(Seq(parentHeader), None,
        genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
      fullHeight = parentHeader.height
      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      withClue("one block of selected-full lag remains eligible: ") {
        rentScans shouldBe 2
      }

      parentHeader = parentHeader.copy(height = height + 2)
      parentBlock = block.copy(parentHeader)
      context = new ErgoStateContext(Seq(parentHeader), None,
        genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
      fullHeight = parentHeader.height

      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      withClue("the extra index is now three selected full blocks behind: ") {
        rentScans shouldBe 2
      }

      parentHeader = selectedHeader
      parentBlock = block.copy(parentHeader)
      context = new ErgoStateContext(Seq(parentHeader), None,
        genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
      fullHeight = indexedHeight
      storage.insertExtraTry(Array(ExtraIndexer.IndexedHeaderIdKey ->
        idToBytes(sideHeader.id)), Array.empty).get
      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      withClue("the indexed header is on a different same-height fork: ") {
        rentScans shouldBe 2
      }

      storage.insertExtraTry(Array(
        ExtraIndexer.IndexedHeightKey -> ByteBuffer.allocate(4).putInt(indexedHeight).array,
        ExtraIndexer.IndexedHeaderIdKey -> idToBytes(selectedHeader.id)
      ), Array.empty).get
      switchCheckpointOnNextFullHeightRead = true
      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings).map(_.isSuccess) shouldBe Some(true)
      withClue("the checkpoint fell three blocks behind between index reads: ") {
        rentScans shouldBe 2
      }
    } finally {
      storage.close()
      state.closeStorage()
    }
  }
}
