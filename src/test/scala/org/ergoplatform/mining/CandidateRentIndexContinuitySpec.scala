package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.ByteBuffer

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, ExtraIndexer, StorageRentBox}
import org.ergoplatform.nodeView.history.storage.HistoryStorage
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
    var parentHeader = block.header.copy(height = height - 1)
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
    storage.insertExtra(Array(ExtraIndexer.IndexedHeightKey ->
      ByteBuffer.allocate(4).putInt(indexedHeight).array), Array.empty[ExtraIndex])
    var fullHeight = indexedHeight
    var rentScans = 0
    val history = Proxy.newProxyInstance(
      classOf[ErgoHistoryReader].getClassLoader,
      Array[Class[_]](classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "historyStorage" => storage
            case "bestFullBlockOpt" => Some(parentBlock)
            case "fullBlockHeight" => Int.box(fullHeight)
            case "storageRentBoxesAtOrBefore" =>
              rentScans += 1
              Array.empty[StorageRentBox]
            case "typedModifierById" => None
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

      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings) should not be None
      rentScans shouldBe 1

      parentHeader = parentHeader.copy(height = height + 2)
      parentBlock = block.copy(parentHeader)
      context = new ErgoStateContext(Seq(parentHeader), None,
        genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
      fullHeight = parentHeader.height

      CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings) should not be None
      withClue("the extra index is now three selected full blocks behind: ") {
        rentScans shouldBe 1
      }
    } finally {
      storage.close()
      state.closeStorage()
    }
  }
}
