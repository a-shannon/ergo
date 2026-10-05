package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, StorageRentBox}
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{BoxHolder, ErgoStateContext, UtxoState, VotingData}
import org.ergoplatform.settings.Constants
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.utils.TestFileUtils
import scorex.crypto.authds.ADKey
import scorex.util.{ModifierId, bytesToId, idToBytes}
import sigmastate.helpers.TestingHelpers._

import org.ergoplatform.utils.generators.ErgoCoreTransactionGenerators._

/** The rent claim is the only available transaction after emission and before new mempool traffic. */
class CandidateRentOnlyGateSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  property("an eligible rent claim can start a candidate without emission or pool transactions") {
    val height = 3 * Constants.StoragePeriod
    val oldBox = testBox(10000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val witnessBox = testBox(10000000000L, Constants.TrueTree,
      height - 2, Seq.empty, Map.empty)
    val unclaimableBox = testBox(1L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))

    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(oldBox, witnessBox, unclaimableBox)), None,
      createTempDir, fixtureSettings, parameters)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }

    val storage = HistoryStorage(fixtureSettings)
    val rentEntry = new StorageRentBox(oldBox.creationHeight, 0L,
      bytesToId(oldBox.id), oldBox.value, oldBox.bytes.length)
    storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])], Array[ExtraIndex](rentEntry))

    var enteredAssembly = false
    var stopAtAssembly = true
    var failIndexRead = false
    val history = Proxy.newProxyInstance(
      classOf[ErgoHistoryReader].getClassLoader,
      Array[Class[_]](classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "bestFullBlockOpt" => Some(parentBlock)
            case "storageRentBoxesUntil" =>
              if (failIndexRead) throw new IllegalStateException("synthetic rent index failure")
              storage.storageRentBoxesUntil(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int])
            case "typedModifierById" =>
              enteredAssembly = true
              if (stopAtAssembly) throw new UnsupportedOperationException("candidate assembly reached")
              else None
            case "requiredDifficultyAfter" => parentHeader.requiredDifficulty
            case other => throw new UnsupportedOperationException(s"unexpected history read: $other")
          }
      }
    ).asInstanceOf[ErgoHistoryReader]

    try {
      val threshold = height - Constants.StoragePeriod
      val indexed = history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
      indexed.map(_.boxId).toSeq shouldBe Seq(bytesToId(oldBox.id))
      val resolved = indexed.flatMap(e => state.boxById(ADKey @@ idToBytes(e.boxId))).toSeq
      resolved shouldBe Seq(oldBox)
      StorageRentClaimBuilder.buildClaim(resolved, height, parameters,
        defaultMinerPk, None, Set.empty[ModifierId]) should not be None
      state.emissionBoxOpt shouldBe None

      val pool = ErgoMemPool.empty(fixtureSettings)
      val witness = ErgoTransaction(
        IndexedSeq(Input(witnessBox.id, emptyProverResult)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(witnessBox.value, Constants.TrueTree, height)))
      val control = CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq(witness), None, fixtureSettings)
      control should not be None
      enteredAssembly shouldBe true

      stopAtAssembly = false
      enteredAssembly = false
      val rentOnly = CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq.empty, None, fixtureSettings)
      withClue("the sole claimable rent box must open candidate assembly: ") {
        rentOnly should not be None
        rentOnly.get.isSuccess shouldBe true
        val candidateTxs = rentOnly.get.get._1.candidateBlock.transactions
        candidateTxs.exists(_.inputs.exists(_.boxId.sameElements(oldBox.id))) shouldBe true
        enteredAssembly shouldBe true
      }

      storage.removeExtra(Array(rentEntry.id))
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims) shouldBe empty
      stopAtAssembly = true
      enteredAssembly = false
      val noClaim = CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq.empty, None, fixtureSettings)
      noClaim shouldBe None
      enteredAssembly shouldBe false

      val unclaimableEntry = new StorageRentBox(unclaimableBox.creationHeight, 1L,
        bytesToId(unclaimableBox.id), unclaimableBox.value, unclaimableBox.bytes.length)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])], Array[ExtraIndex](unclaimableEntry))
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId).toSeq shouldBe Seq(bytesToId(unclaimableBox.id))
      StorageRentClaimBuilder.buildClaim(Seq(unclaimableBox), height, parameters,
        defaultMinerPk, None, Set.empty[ModifierId]) shouldBe None
      stopAtAssembly = false
      enteredAssembly = false
      val indexedButUnclaimable = CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq.empty, None, fixtureSettings)
      indexedButUnclaimable shouldBe None
      enteredAssembly shouldBe true

      failIndexRead = true
      val failedIndex = CandidateGenerator.generateCandidate(history, state, pool,
        defaultMinerPk, Seq.empty, None, fixtureSettings)
      failedIndex should not be None
      failedIndex.get.isFailure shouldBe true
      failedIndex.get.failed.get.getMessage shouldBe "synthetic rent index failure"
    } finally {
      storage.close()
      state.closeStorage()
    }
  }
}
