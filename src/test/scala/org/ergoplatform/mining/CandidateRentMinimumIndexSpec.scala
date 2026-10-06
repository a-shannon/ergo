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

  private def checkUnclaimablePrefix(prefixCount: Int,
                                     trailingCount: Int = 0,
                                     recheckClaim: Boolean = false,
                                     earlierClaim: Boolean = false,
                                     policyReset: Boolean = false,
                                     rollbackRevisit: Boolean = false): Unit = {
    val height = 3 * Constants.StoragePeriod
    val tokens = (1 to 10).map(i =>
      (Digest32Coll @@ Colls.fromArray(Array.fill(32)(i.toByte))) -> 5L)
    def boxAt(value: Long, age: Int): ErgoBox =
      testBox(value, Constants.TrueTree, height - age, tokens, Map.empty)
    def minimum(box: ErgoBox): Long = parameters.minValuePerByte.toLong * box.bytes.length
    def minimumTokenBox(age: Int): ErgoBox = {
      var b = boxAt(10000000000L, age)
      var attempts = 0
      while (b.value > minimum(b) && attempts < 20) {
        b = boxAt(minimum(b), age)
        attempts += 1
      }
      b
    }
    val box = minimumTokenBox(Constants.StoragePeriod)
    box.value should be <= minimum(box)
    def plainBoxAt(value: Long, age: Int): ErgoBox =
      testBox(value, Constants.TrueTree, height - age,
        Seq.empty, Map.empty)
    def unclaimableAt(age: Int): ErgoBox = {
      var b = plainBoxAt(10000000000L, age)
      while (b.value > minimum(b)) {
        b = plainBoxAt(minimum(b), age)
      }
      b
    }
    val dustOnlyBoxes = (1 to prefixCount)
      .map(i => unclaimableAt(Constants.StoragePeriod + i))
    val firstClaim = if (earlierClaim || rollbackRevisit)
      Seq(plainBoxAt(10000000000L, Constants.StoragePeriod + prefixCount + 1))
    else if (policyReset)
      Seq(minimumTokenBox(Constants.StoragePeriod + prefixCount + 1))
    else Seq.empty
    val trailingBoxes = (1 to trailingCount)
      .map(_ => unclaimableAt(Constants.StoragePeriod))
    val dustOnly = dustOnlyBoxes.head
    StorageRentClaimBuilder.buildClaim(Seq(dustOnly), height, parameters,
      defaultMinerPk, None, Set.empty).isEmpty shouldBe true
    val unrelated = testBox(10000000000L, Constants.TrueTree, height,
      Seq.empty, Map.empty)
    val unrelatedSpend = ErgoTransaction(
      IndexedSeq(Input(unrelated.id, emptyProverResult)), IndexedSeq.empty,
      IndexedSeq(new ErgoBoxCandidate(unrelated.value, Constants.TrueTree, height)))

    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true,
        storageRentCollection = true,
        storageRentTokenWhitelist = if (policyReset)
          Seq(bytesToId(Array.fill(32)(1.toByte))) else Seq.empty))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(box, unrelated) ++ firstClaim ++ dustOnlyBoxes ++ trailingBoxes), None,
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
    (firstClaim ++ dustOnlyBoxes ++ Seq(box) ++ trailingBoxes).zipWithIndex.foreach { case (b, index) =>
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
            case "bestFullBlockAt" => Some(parentBlock)
            case "isInSelectedFullChain" => Boolean.box(args(0) == parentHeader.id)
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
      history.storageRentBoxesUntil(threshold, Int.MaxValue)
        .map(_.boxId) should contain(bytesToId(box.id))
      val firstPage = history.storageRentBoxesUntil(threshold, StorageRentClaimBuilder.MaxClaims)
      firstPage.length shouldBe StorageRentClaimBuilder.MaxClaims
      firstPage.map(_.boxId).toSet shouldBe
        (firstClaim ++ dustOnlyBoxes ++ Seq(box) ++ trailingBoxes).zipWithIndex
          .sortBy { case (b, index) => (b.creationHeight, index) }
          .take(StorageRentClaimBuilder.MaxClaims)
          .map { case (b, _) => bytesToId(b.id) }.toSet
      if (rollbackRevisit)
        history.removeStorageRentBoxes(Seq(bytesToId(firstClaim.head.id)))
      val (firstResult, progress) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
        Seq(unrelatedSpend), None, fixtureSettings, CandidateGenerator.RentScanState())
      firstResult should not be None
      firstResult.get.isSuccess shouldBe true
      if (rollbackRevisit) {
        progress.state.after should not be None
        val restored = new IndexedErgoBox(firstClaim.head.creationHeight,
          None, None, None, firstClaim.head, 0L)
        storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
          Array[ExtraIndex](StorageRentBox(restored)))
        val oldTip = bytesToId(Array.fill(32)(0x7f.toByte))
        val rolledBack = progress.state.copy(tip = Some(parentHeader.height -> oldTip))
        val (restoredResult, _) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq(unrelatedSpend), None, fixtureSettings, rolledBack)
        restoredResult should not be None
        withClue(s"restored candidate: ${restoredResult.get}") {
          restoredResult.get.isSuccess shouldBe true
        }
        restoredResult.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(firstClaim.head.id))) shouldBe true
        return
      }
      if (policyReset) {
        progress.state.after should not be None
        firstResult.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(firstClaim.head.id))) shouldBe false
        val changedSettings = fixtureSettings.copy(nodeSettings =
          fixtureSettings.nodeSettings.copy(storageRentTokenWhitelist = Seq.empty))
        val (changedResult, _) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq(unrelatedSpend), None, changedSettings, progress.state)
        changedResult should not be None
        changedResult.get.isSuccess shouldBe true
        changedResult.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(firstClaim.head.id))) shouldBe true
        return
      }
      val resultAfterEarlyClaim = if (earlierClaim) {
        firstResult.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(firstClaim.head.id))) shouldBe true
        firstResult.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(box.id))) shouldBe false
        progress.more shouldBe true
        val (laterResult, laterProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq(unrelatedSpend), None, fixtureSettings, progress.state)
        laterResult should not be None
        laterResult.get.isSuccess shouldBe true
        val laterIds = laterResult.get.get._1.candidateBlock.transactions.flatMap(_.inputs)
          .map(_.boxId.toSeq)
        laterIds should contain(firstClaim.head.id.toSeq)
        laterIds should contain(box.id.toSeq)
        laterProgress.state.after should not be progress.state.after
        laterResult
      } else firstResult
      val result = if (prefixCount > 2 * StorageRentClaimBuilder.MaxClaims) {
        val (firstCandidate, _) = firstResult.get.get
        firstCandidate.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(box.id))) shouldBe false
        progress.more shouldBe true
        CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq(unrelatedSpend), None, fixtureSettings, progress.state)._1
      } else resultAfterEarlyClaim
      result should not be None
      result.get.isSuccess shouldBe true
      val (candidate, _) = result.get.get
      candidate.candidateBlock.transactions.exists(_.inputs.exists(_.boxId.sameElements(box.id))) shouldBe true
      if (recheckClaim) {
        progress.more shouldBe true
        val repeated = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq(unrelatedSpend), None, fixtureSettings, progress.state)._1
        repeated should not be None
        repeated.get.isSuccess shouldBe true
        repeated.get.get._1.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(box.id))) shouldBe true
      }
      history.storageRentBoxesUntil(threshold, Int.MaxValue)
        .map(_.boxId) should contain(bytesToId(box.id))
      history.storageRentBoxesUntil(threshold, Int.MaxValue)
        .map(_.boxId) should contain(bytesToId(dustOnly.id))
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("candidate reaches a claimable box after ten older unclaimable index rows") {
    checkUnclaimablePrefix(StorageRentClaimBuilder.MaxClaims)
  }

  property("candidate cursor reaches beyond two bounded pages without deleting older rows") {
    checkUnclaimablePrefix(2 * StorageRentClaimBuilder.MaxClaims + 1)
  }

  property("a speculative rent claim remains in the next candidate until spent") {
    checkUnclaimablePrefix(1, trailingCount = 25, recheckClaim = true)
  }

  property("a speculative early claim does not block a later eligible claim") {
    checkUnclaimablePrefix(StorageRentClaimBuilder.MaxClaims, earlierClaim = true)
  }

  property("a policy change revisits a previously skipped row behind the cursor") {
    checkUnclaimablePrefix(2 * StorageRentClaimBuilder.MaxClaims + 5,
      policyReset = true)
  }

  property("a selected-chain rollback revisits a restored row behind the cursor") {
    checkUnclaimablePrefix(2 * StorageRentClaimBuilder.MaxClaims + 5,
      rollbackRevisit = true)
  }
}
