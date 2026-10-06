package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, IndexedErgoBox, StorageRentBox}
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

/** A candidate-local claim conflict must not erase independent unspent boxes or invalidate the mempool. */
class CandidateRentRejectIndexSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  property("prioritized owner spend does not drop other boxes in a rejected rent claim") {
    val height = 3 * Constants.StoragePeriod
    val spentByOwner = testBox(10000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val stillUnspent = testBox(11000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))

    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(spentByOwner, stillUnspent)), None,
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
    Seq(spentByOwner, stillUnspent).zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None, box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, StorageRentBox(indexed)))
    }

    val history = Proxy.newProxyInstance(
      classOf[ErgoHistoryReader].getClassLoader,
      Array[Class[_]](classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "bestFullBlockOpt" => Some(parentBlock)
            case "storageRentBoxesUntil" =>
              storage.storageRentBoxesUntil(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int])
            case "storageRentBoxesAfter" =>
              storage.storageRentBoxesAfter(args(0).asInstanceOf[Int],
                args(1).asInstanceOf[Int], args(2).asInstanceOf[Option[(Int, Long)]])
            case "storageRentBoxesPage" =>
              storage.storageRentBoxesPage(args(0).asInstanceOf[Int],
                args(1).asInstanceOf[Int], args(2).asInstanceOf[Option[Vector[Byte]]])
            case "removeStorageRentBoxes" =>
              storage.removeStorageRentBoxes(args(0).asInstanceOf[Seq[ModifierId]])
              ().asInstanceOf[AnyRef]
            case "typedModifierById" => None
            case "requiredDifficultyAfter" => parentHeader.requiredDifficulty
            case other => throw new UnsupportedOperationException(s"unexpected history read: $other")
          }
      }
    ).asInstanceOf[ErgoHistoryReader]

    try {
      val threshold = height - Constants.StoragePeriod
      val before = history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
      before.map(_.boxId).toSet shouldBe Set(bytesToId(spentByOwner.id), bytesToId(stillUnspent.id))
      val batch = StorageRentClaimBuilder.buildClaim(Seq(spentByOwner, stillUnspent),
        height, parameters, defaultMinerPk, None, Set.empty[ModifierId]).get
      batch.inputs.map(in => bytesToId(in.boxId)).toSet shouldBe before.map(_.boxId).toSet

      val ownerSpend = ErgoTransaction(
        IndexedSeq(Input(spentByOwner.id, emptyProverResult)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(spentByOwner.value, Constants.TrueTree, height)))
      val result = CandidateGenerator.generateCandidate(history, state,
        ErgoMemPool.empty(fixtureSettings), defaultMinerPk, Seq(ownerSpend), None, fixtureSettings)
      result should not be None
      result.get.isSuccess shouldBe true
      val (candidate, rejected) = result.get.get
      candidate.candidateBlock.transactions.map(_.id) should contain(ownerSpend.id)
      candidate.candidateBlock.transactions.map(_.id) should not contain batch.id
      // The claim is speculative candidate work, not a mempool transaction to evict.
      rejected.ids should not contain batch.id

      // A was spent only in this candidate; B was never spent at all. It must remain
      // available for the next candidate even when the combined claim was rejected.
      state.boxById(ADKey @@ idToBytes(bytesToId(stillUnspent.id))) shouldBe Some(stillUnspent)
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId).toSet should contain(bytesToId(stillUnspent.id))
    } finally {
      storage.close()
      state.closeStorage()
    }
  }
}
