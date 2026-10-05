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
import scorex.util.{ModifierId, bytesToId}
import sigma.Colls
import sigma.data.Digest32Coll
import sigmastate.helpers.TestingHelpers._

import org.ergoplatform.utils.generators.ErgoCoreTransactionGenerators._

/** A minimum-valued tokenized box may still be collected through the rent-burn path. */
class CandidateRentMinimumIndexSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  property("candidate collects a minimum-valued tokenized box without deleting its rent index") {
    val height = 3 * Constants.StoragePeriod
    val tokens = (1 to 10).map(i =>
      (Digest32Coll @@ Colls.fromArray(Array.fill(32)(i.toByte))) -> 5L)
    def boxAt(value: Long): ErgoBox =
      testBox(value, Constants.TrueTree, height - Constants.StoragePeriod, tokens, Map.empty)
    def minimum(box: ErgoBox): Long = parameters.minValuePerByte.toLong * box.bytes.length
    var box = boxAt(10000000000L)
    var attempts = 0
    while (box.value > minimum(box) && attempts < 20) {
      box = boxAt(minimum(box))
      attempts += 1
    }
    box.value should be <= minimum(box)
    def plainBoxAt(value: Long): ErgoBox =
      testBox(value, Constants.TrueTree, height - Constants.StoragePeriod,
        Seq.empty, Map.empty)
    var dustOnly = plainBoxAt(10000000000L)
    while (dustOnly.value > minimum(dustOnly)) {
      dustOnly = plainBoxAt(minimum(dustOnly))
    }
    StorageRentClaimBuilder.buildClaim(Seq(dustOnly), height, parameters,
      defaultMinerPk, None, Set.empty).isEmpty shouldBe true
    val unrelated = testBox(10000000000L, Constants.TrueTree, height,
      Seq.empty, Map.empty)
    val unrelatedSpend = ErgoTransaction(
      IndexedSeq(Input(unrelated.id, emptyProverResult)), IndexedSeq.empty,
      IndexedSeq(new ErgoBoxCandidate(unrelated.value, Constants.TrueTree, height)))

    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(box, unrelated, dustOnly)), None,
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
    Seq(box, dustOnly).zipWithIndex.foreach { case (b, index) =>
      val indexed = new IndexedErgoBox(b.creationHeight, None, None, None, b, index.toLong)
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
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId) should contain(bytesToId(box.id))
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId) should contain(bytesToId(dustOnly.id))
      val result = CandidateGenerator.generateCandidate(history, state,
        ErgoMemPool.empty(fixtureSettings), defaultMinerPk, Seq(unrelatedSpend), None, fixtureSettings)
      result should not be None
      result.get.isSuccess shouldBe true
      val (candidate, _) = result.get.get
      candidate.candidateBlock.transactions.exists(_.inputs.exists(_.boxId.sameElements(box.id))) shouldBe true
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId) should contain(bytesToId(box.id))
      history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
        .map(_.boxId) should contain(bytesToId(dustOnly.id))
    } finally {
      storage.close()
      state.closeStorage()
    }
  }
}
