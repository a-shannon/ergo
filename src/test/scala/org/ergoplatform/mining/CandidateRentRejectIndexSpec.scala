package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, IndexedErgoBox, NumericBoxIndex, StorageRentBox}
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{BoxHolder, ErgoStateContext, UtxoState, VotingData}
import org.ergoplatform.settings.{Constants, Parameters}
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.interpreter.ErgoInterpreter
import org.ergoplatform.wallet.utils.TestFileUtils
import scorex.crypto.authds.ADKey
import scorex.util.{ModifierId, bytesToId, idToBytes}
import sigma.ast.ByteArrayConstant
import sigmastate.helpers.TestingHelpers._

import org.ergoplatform.utils.generators.ErgoCoreTransactionGenerators._

/** A rejected batch claim must not erase eligibility of independent unspent boxes. */
class CandidateRentRejectIndexSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  property("prioritized owner spend does not drop other boxes in a rejected rent claim") {
    val height = 3 * Constants.StoragePeriod
    val spentByOwner = testBox(10000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val stillUnspent = testBox(11000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val indexedLater = testBox(12000000000L, Constants.TrueTree,
      height - Constants.StoragePeriod, Seq.empty, Map.empty)
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(Seq(spentByOwner, stillUnspent, indexedLater)), None,
      createTempDir, fixtureSettings, parameters)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, parameters, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    Seq(spentByOwner, stillUnspent).zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None, box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id), StorageRentBox(indexed)))
    }
    // Keep the first page nonterminal. These history rows have numeric and
    // box records, but their boxes are absent from the current UTXO state.
    val filteredRows = (2L to 100L).flatMap { index =>
      val box = testBox(index, Constants.TrueTree, spentByOwner.creationHeight,
        Seq.empty, Map.empty)
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None, box, index)
      Seq[ExtraIndex](indexed, NumericBoxIndex(index, indexed.id), StorageRentBox(indexed))
    }.toArray
    storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])], filteredRows)
    val history = historyProxy(storage, parentBlock)

    try {
      val threshold = height - Constants.StoragePeriod
      def rentBoxIds: Set[ModifierId] =
        history.storageRentBoxesAtOrBefore(threshold, StorageRentClaimBuilder.MaxClaims)
          .flatMap(entry => NumericBoxIndex.getBoxByNumber(history, entry.globalIndex))
          .map(_.id).filter(id =>
            state.boxById(ADKey @@ idToBytes(id)).nonEmpty).toSet
      val before = rentBoxIds
      before shouldBe Set(bytesToId(spentByOwner.id), bytesToId(stillUnspent.id))
      val batch = StorageRentClaimBuilder.buildClaim(Seq(spentByOwner, stillUnspent),
        height, parameters, defaultMinerPk, None, Set.empty[ModifierId]).get
      batch.inputs.map(in => bytesToId(in.boxId)).toSet shouldBe before

      val ownerSpend = ErgoTransaction(
        IndexedSeq(Input(spentByOwner.id, emptyProverResult)),
        IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(spentByOwner.value, Constants.TrueTree, height)))
      val (result, progress) = CandidateGenerator.generateCandidateWithRentScan(history, state,
        ErgoMemPool.empty(fixtureSettings), defaultMinerPk, Seq(ownerSpend), None,
        fixtureSettings, CandidateGenerator.RentScanState())
      result should not be None
      result.get.isSuccess shouldBe true
      val (candidate, rejected) = result.get.get
      candidate.candidateBlock.transactions.map(_.id) should contain(ownerSpend.id)
      candidate.candidateBlock.transactions.map(_.id) should not contain batch.id
      rejected.ids should contain(batch.id)
      progress.state.pendingClaims.flatten.toSet shouldBe before
      progress.state.after should not be None

      // Index catch-up can add a later claim while the first group is rejected.
      val later = new IndexedErgoBox(indexedLater.creationHeight, None, None, None,
        indexedLater, 101L)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](later, NumericBoxIndex(101L, later.id), StorageRentBox(later)))
      val (retried, afterRetry) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
        Seq(ownerSpend), None, fixtureSettings, progress.state)
      retried.get.isSuccess shouldBe true
      retried.get.get._2.ids should contain(batch.id)
      afterRetry.state.pendingClaims.flatten.toSet shouldBe Set(bytesToId(stillUnspent.id))
      val (remainder, afterRemainder) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
        Seq(ownerSpend), None, fixtureSettings, afterRetry.state)
      remainder.get.isSuccess shouldBe true
      remainder.get.get._1.candidateBlock.transactions.exists(
        _.inputs.exists(_.boxId.sameElements(stillUnspent.id))) shouldBe true
      afterRemainder.state.pendingClaims shouldBe empty
      val (advanced, afterAdvance) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
        Seq(ownerSpend), None, fixtureSettings, afterRemainder.state)
      advanced.get.isSuccess shouldBe true
      advanced.get.get._1.candidateBlock.transactions.exists(
        _.inputs.exists(_.boxId.sameElements(indexedLater.id))) shouldBe true
      afterAdvance.state.after should not be afterRemainder.state.after

      state.boxById(ADKey @@ idToBytes(bytesToId(stillUnspent.id))) shouldBe Some(stillUnspent)
      rentBoxIds should contain(bytesToId(stillUnspent.id))

      // When a prioritized owner spend rejects the claim, ordinary conflicts
      // retain the existing invalidation behavior.
      val competingSpend = ErgoTransaction(
        IndexedSeq(Input(spentByOwner.id, emptyProverResult)),
        IndexedSeq.empty,
        IndexedSeq(
          new ErgoBoxCandidate(spentByOwner.value / 2, Constants.TrueTree, height),
          new ErgoBoxCandidate(spentByOwner.value - spentByOwner.value / 2,
            Constants.TrueTree, height)))
      val rejectedClaimPool = ErgoMemPool.empty(fixtureSettings)
        .put(UnconfirmedTransaction(competingSpend, None))
      val (rejectedClaimAttempt, _) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, rejectedClaimPool, defaultMinerPk, Seq(ownerSpend), None,
        fixtureSettings, CandidateGenerator.RentScanState())
      rejectedClaimAttempt.get.isSuccess shouldBe true
      val (rejectedClaimCandidate, rejectedClaimEliminated) = rejectedClaimAttempt.get.get
      rejectedClaimCandidate.candidateBlock.transactions.map(_.id) should contain(ownerSpend.id)
      rejectedClaimCandidate.candidateBlock.transactions.map(_.id) should not contain batch.id
      rejectedClaimEliminated.ids should contain(competingSpend.id)

      // Rent wins this candidate, but the ordinary owner spend is still valid
      // against the selected state until a claim is actually mined.
      val missingInput = Array.fill[Byte](32)(42.toByte)
      val independentlyInvalid = ErgoTransaction(
        IndexedSeq(Input(ADKey @@ missingInput, emptyProverResult)),
        IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(spentByOwner.value, Constants.TrueTree, height)))
      val descendant = ErgoTransaction(
        IndexedSeq(Input(ownerSpend.outputs.head.id, emptyProverResult)),
        IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(spentByOwner.value, Constants.TrueTree, height)))
      val pool = ErgoMemPool.empty(fixtureSettings).put(Seq(
        UnconfirmedTransaction(ownerSpend, None),
        UnconfirmedTransaction(descendant, None),
        UnconfirmedTransaction(independentlyInvalid, None)))
      val (poolAttempt, _) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, pool, defaultMinerPk, Seq.empty, None,
        fixtureSettings, CandidateGenerator.RentScanState())
      poolAttempt.get.isSuccess shouldBe true
      val (poolCandidate, poolEliminated) = poolAttempt.get.get
      poolCandidate.candidateBlock.transactions.map(_.id) should contain(batch.id)
      poolCandidate.candidateBlock.transactions.map(_.id) should not contain(ownerSpend.id)
      poolCandidate.candidateBlock.transactions.map(_.id) should not contain descendant.id
      poolEliminated.ids should contain(independentlyInvalid.id)
      poolEliminated.ids should not contain ownerSpend.id
      poolEliminated.ids should not contain descendant.id
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("dust-sized rent fees accumulate across bounded raw-key pages") {
    val height = 3 * Constants.StoragePeriod
    val creationHeight = height - Constants.StoragePeriod
    val dustParams = Parameters(parameters.height, parameters.parametersTable
      .updated(Parameters.StorageFeeFactorIncrease, Parameters.StorageFeeFactorMax)
      .updated(Parameters.MinValuePerByteIncrease, Parameters.MinValueMax),
      parameters.proposedUpdate)
    val payloadLength = (0 to 1718).find { length =>
      testBox(100000000L, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](length)(1)))).bytes.length == 1718
    }.get
    val boxes = (0 until StorageRentClaimBuilder.MaxClaims).map { index =>
      testBox(100000000L + index, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](payloadLength)(index.toByte))))
    }
    boxes.foreach(_.bytes.length shouldBe 1718)
    (dustParams.storageFeeFactor * boxes.head.bytes.length) shouldBe 32704
    val claimCount = (2 to StorageRentClaimBuilder.MaxClaims).find { count =>
      val first = boxes.take(count / 2)
      val second = boxes.slice(count / 2, count)
      StorageRentClaimBuilder.buildClaim(boxes.take(count), height, dustParams,
        defaultMinerPk, None, Set.empty[ModifierId]).nonEmpty &&
        StorageRentClaimBuilder.buildClaim(first, height, dustParams,
          defaultMinerPk, None, Set.empty[ModifierId]).isEmpty &&
        StorageRentClaimBuilder.buildClaim(second, height, dustParams,
          defaultMinerPk, None, Set.empty[ModifierId]).isEmpty
    }.get
    val claimable = boxes.take(claimCount)
    val firstPageCount = claimCount / 2
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(claimable), None,
      createTempDir, fixtureSettings, dustParams)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, dustParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    claimable.zipWithIndex.foreach { case (box, offset) =>
      val index = if (offset < firstPageCount) offset.toLong else 200L + offset
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None, box, index)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index, indexed.id), StorageRentBox(indexed)))
    }
    val foreignKeys = (firstPageCount until 200).map { index =>
      StorageRentBox.key(creationHeight, index.toLong) ++ Array.fill[Byte](19)(0)
    }
    storage.insertExtra(foreignKeys.map(_ -> Array[Byte](1)).toArray, Array.empty[ExtraIndex])
    val history = historyProxy(storage, parentBlock)

    try {
      storage.storageRentBoxesAtOrBefore(creationHeight, 100).length shouldBe claimCount
      val firstPage = storage.storageRentBoxesPage(creationHeight, 100, None)
      firstPage.rows.length shouldBe firstPageCount
      firstPage.hasMore shouldBe true
      val secondPage = storage.storageRentBoxesPage(creationHeight, 100, firstPage.after)
      secondPage.rows shouldBe empty
      secondPage.hasMore shouldBe true
      val thirdPage = storage.storageRentBoxesPage(creationHeight, 100, secondPage.after)
      thirdPage.rows.length shouldBe claimCount - firstPageCount
      val claimableIds = claimable.map(box => bytesToId(box.id)).toSet
      var scan = CandidateGenerator.RentScanState()
      var claimed = false
      (1 to 3).foreach { _ =>
        val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq.empty, None, fixtureSettings, scan)
        claimed = claimed || attempt.flatMap(_.toOption).exists { case (candidate, _) =>
          candidate.candidateBlock.transactions.exists { tx =>
            tx.inputs.exists(in => claimableIds.contains(bytesToId(in.boxId)))
          }
        }
        scan = progress.state
      }
      claimed shouldBe true
      storage.storageRentBoxesAtOrBefore(creationHeight, 100).length shouldBe claimCount
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("minimum-value rows do not displace dust contributors between raw-key pages") {
    val height = 3 * Constants.StoragePeriod
    val creationHeight = height - Constants.StoragePeriod
    val dustParams = Parameters(parameters.height, parameters.parametersTable
      .updated(Parameters.StorageFeeFactorIncrease, Parameters.StorageFeeFactorMax)
      .updated(Parameters.MinValuePerByteIncrease, Parameters.MinValueMax),
      parameters.proposedUpdate)
    val payloadLength = (0 to 1718).find { length =>
      testBox(100000000L, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](length)(1)))).bytes.length == 1718
    }.get
    def agedBox(value: Long, index: Int): ErgoBox =
      testBox(value, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](payloadLength)(index.toByte))))
    val contributors = (0 until StorageRentClaimBuilder.MaxClaims)
      .map(index => agedBox(100000000L + index, index))
    val claimCount = (2 to StorageRentClaimBuilder.MaxClaims).find { count =>
      StorageRentClaimBuilder.buildClaim(contributors.take(count), height, dustParams,
        defaultMinerPk, None, Set.empty[ModifierId]).nonEmpty &&
        StorageRentClaimBuilder.buildClaim(contributors.take(count - 1), height, dustParams,
          defaultMinerPk, None, Set.empty[ModifierId]).isEmpty
    }.get
    val minimumValue = dustParams.minValuePerByte.toLong * contributors.head.bytes.length
    val fillers = (100 until 300).map(index => agedBox(minimumValue, index))
    val firstPage = contributors.take(claimCount - 1) ++
      fillers.take(StorageRentClaimBuilder.MaxClaims - claimCount + 1)
    val secondPage = Seq(contributors(claimCount - 1)) ++
      fillers.slice(StorageRentClaimBuilder.MaxClaims - claimCount + 1,
        2 * StorageRentClaimBuilder.MaxClaims - claimCount)
    val indexedBoxes = firstPage ++ secondPage
    indexedBoxes.size shouldBe 2 * StorageRentClaimBuilder.MaxClaims
    indexedBoxes.foreach(_.bytes.length shouldBe 1718)
    fillers.foreach(_.value shouldBe dustParams.minValuePerByte.toLong * 1718)
    StorageRentClaimBuilder.buildClaim(firstPage, height, dustParams,
      defaultMinerPk, None, Set.empty[ModifierId]) shouldBe None
    StorageRentClaimBuilder.buildClaim(secondPage, height, dustParams,
      defaultMinerPk, None, Set.empty[ModifierId]) shouldBe None

    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(indexedBoxes), None,
      createTempDir, fixtureSettings, dustParams)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, dustParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    indexedBoxes.zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
        box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
          StorageRentBox(indexed)))
    }
    val history = historyProxy(storage, parentBlock)

    try {
      val first = storage.storageRentBoxesPage(creationHeight, 100, None)
      first.rows.length shouldBe 100
      first.hasMore shouldBe true
      val second = storage.storageRentBoxesPage(creationHeight, 100, first.after)
      second.rows.length shouldBe 100
      second.hasMore shouldBe false
      val contributorIds = contributors.take(claimCount).map(box => bytesToId(box.id)).toSet
      var scan = CandidateGenerator.RentScanState()
      var offered = Set.empty[ModifierId]
      (1 to 3).foreach { _ =>
        val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq.empty, None, fixtureSettings, scan)
        offered ++= attempt.flatMap(_.toOption).toSeq.flatMap(_._1.candidateBlock.transactions)
          .flatMap(_.inputs).map(in => bytesToId(in.boxId)).filter(contributorIds.contains)
        progress.state.dustCarry.size should be <= StorageRentClaimBuilder.MaxClaims
        scan = progress.state
      }
      offered shouldBe contributorIds
      storage.storageRentBoxesAtOrBefore(creationHeight, 250).length shouldBe indexedBoxes.size
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("smaller positive payouts do not displace a viable cross-page rent group") {
    val height = 3 * Constants.StoragePeriod
    val creationHeight = height - Constants.StoragePeriod
    val dustParams = Parameters(parameters.height, parameters.parametersTable
      .updated(Parameters.StorageFeeFactorIncrease, Parameters.StorageFeeFactorMax)
      .updated(Parameters.MinValuePerByteIncrease, Parameters.MinValueMax),
      parameters.proposedUpdate)
    val payloadLength = (0 to 1718).find { length =>
      testBox(100000000L, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](length)(1)))).bytes.length == 1718
    }.get
    def agedBox(value: Long, index: Int): ErgoBox =
      testBox(value, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](payloadLength)(index.toByte))))
    val high = (0 until StorageRentClaimBuilder.MaxClaims)
      .map(index => agedBox(100000000L + index, index))
    val thresholdCount = (2 to StorageRentClaimBuilder.MaxClaims).find { count =>
      StorageRentClaimBuilder.buildClaim(high.take(count), height, dustParams,
        defaultMinerPk, None, Set.empty[ModifierId]).nonEmpty &&
        StorageRentClaimBuilder.buildClaim(high.take(count - 1), height, dustParams,
          defaultMinerPk, None, Set.empty[ModifierId]).isEmpty
    }.get
    val highGroup = high.take(thresholdCount - 1)
    val minimumValue = dustParams.minValuePerByte.toLong * 1718
    val firstPage = highGroup ++ (100 until 100 + 101 - thresholdCount)
      .map(index => agedBox(minimumValue, index))
    val lowPage = Seq(500L, 1000L, 2000L, 4000L).map { bonus =>
      (200 until 300).map(index => agedBox(minimumValue + bonus, index))
    }.find { low =>
      StorageRentClaimBuilder.buildClaim(low, height, dustParams,
        defaultMinerPk, None, Set.empty[ModifierId]).isEmpty &&
        StorageRentClaimBuilder.buildClaim(highGroup ++ low.take(100 - highGroup.size),
          height, dustParams, defaultMinerPk, None, Set.empty[ModifierId]).nonEmpty
    }.get
    val indexedBoxes = firstPage ++ lowPage
    firstPage.size shouldBe 100
    lowPage.size shouldBe 100
    indexedBoxes.foreach(_.bytes.length shouldBe 1718)
    StorageRentClaimBuilder.payoutContributions(lowPage, height, dustParams,
      defaultMinerPk, None, Set.empty[ModifierId]).size shouldBe 100

    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(indexedBoxes), None,
      createTempDir, fixtureSettings, dustParams)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, dustParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    indexedBoxes.zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
        box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
          StorageRentBox(indexed)))
    }
    val history = historyProxy(storage, parentBlock)

    try {
      val first = storage.storageRentBoxesPage(creationHeight, 100, None)
      first.rows.length shouldBe 100
      first.hasMore shouldBe true
      val second = storage.storageRentBoxesPage(creationHeight, 100, first.after)
      second.rows.length shouldBe 100
      second.hasMore shouldBe false
      val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
        Seq.empty, None, fixtureSettings, CandidateGenerator.RentScanState())
      val offered = attempt.flatMap(_.toOption).toSeq.flatMap(_._1.candidateBlock.transactions)
        .flatMap(_.inputs).map(in => bytesToId(in.boxId)).toSet
      offered.intersect(highGroup.map(box => bytesToId(box.id)).toSet).size shouldBe highGroup.size
      progress.state.dustCarry.size should be <= StorageRentClaimBuilder.MaxClaims
      storage.storageRentBoxesAtOrBefore(creationHeight, 250).length shouldBe indexedBoxes.size
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("a cost-limited rent group is split until an affordable claim is offered") {
    val height = 3 * Constants.StoragePeriod
    val limitedParams = Parameters(parameters.height,
      parameters.parametersTable.updated(Parameters.MaxBlockCostIncrease, 16 * 1024),
      parameters.proposedUpdate)
    val agedBoxes = (0 until StorageRentClaimBuilder.MaxClaims).map { index =>
      testBox(10000000000L + index, Constants.TrueTree,
        height - Constants.StoragePeriod, Seq.empty, Map.empty)
    }
    val witnessBox = testBox(10000000000L, Constants.TrueTree,
      height - 2, Seq.empty, Map.empty)
    val witness = ErgoTransaction(
      IndexedSeq(Input(witnessBox.id, emptyProverResult)),
      IndexedSeq.empty,
      IndexedSeq(new ErgoBoxCandidate(witnessBox.value, Constants.TrueTree, height)))
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(agedBoxes :+ witnessBox), None,
      createTempDir, fixtureSettings, limitedParams)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, limitedParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    agedBoxes.zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
        box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
          StorageRentBox(indexed)))
    }
    val history = historyProxy(storage, parentBlock)

    try {
      val claim = StorageRentClaimBuilder.buildClaim(agedBoxes, height, limitedParams,
        defaultMinerPk, None, Set.empty[ModifierId]).get
      claim.inputs.size shouldBe StorageRentClaimBuilder.MaxClaims
      claim.size should be < limitedParams.maxBlockSize
      limitedParams.inputCost * claim.inputs.size should be > limitedParams.maxBlockCost
      val pool = ErgoMemPool.empty(fixtureSettings)
        .put(UnconfirmedTransaction(witness, None))
      val noRentSettings = fixtureSettings.copy(nodeSettings = fixtureSettings.nodeSettings
        .copy(storageRentCollection = false))
      val (control, _) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, pool, defaultMinerPk, Seq.empty, None,
        noRentSettings, CandidateGenerator.RentScanState())
      control.get.isSuccess shouldBe true
      control.get.get._1.candidateBlock.transactions.map(_.id) should contain(witness.id)

      val agedIds = agedBoxes.map(box => bytesToId(box.id)).toSet
      var scan = CandidateGenerator.RentScanState()
      var offeredIds = Set.empty[ModifierId]
      var attempts = 0
      while (attempts < 250 && offeredIds != agedIds) {
        val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, pool, defaultMinerPk, Seq.empty, None,
          fixtureSettings, scan)
        attempt.get.isSuccess shouldBe true
        val candidate = attempt.get.get._1.candidateBlock
        val offered = candidate.transactions.flatMap(_.inputs)
          .map(in => bytesToId(in.boxId)).filter(agedIds.contains)
        if (offered.isEmpty) candidate.transactions.map(_.id) should contain(witness.id)
        offeredIds ++= offered
        progress.state.pendingClaims.flatten.size should be <= StorageRentClaimBuilder.MaxClaims
        if (attempts == 0) {
          progress.state.pendingClaims.map(_.size) shouldBe Seq(StorageRentClaimBuilder.MaxClaims)
          progress.state.rejectedAttempts shouldBe 1
        } else if (attempts == 1) {
          progress.state.pendingClaims.map(_.size) shouldBe Seq(50, 50)
          progress.state.rejectedAttempts shouldBe 0
        }
        scan = progress.state
        attempts += 1
      }
      offeredIds shouldBe agedIds
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("a payable non-dyadic dust group survives cost-driven claim splitting") {
    val height = 3 * Constants.StoragePeriod
    val creationHeight = height - Constants.StoragePeriod
    val dustParams = Parameters(parameters.height, parameters.parametersTable
      .updated(Parameters.StorageFeeFactorIncrease, Parameters.StorageFeeFactorMax)
      .updated(Parameters.MinValuePerByteIncrease, Parameters.MinValueMax),
      parameters.proposedUpdate)
    val payloadLength = (0 to 1718).find { length =>
      testBox(100000000L, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](length)(1)))).bytes.length == 1718
    }.get
    val agedBoxes = (0 until StorageRentClaimBuilder.MaxClaims).map { index =>
      testBox(100000000L + index, Constants.TrueTree, creationHeight, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](payloadLength)(index.toByte))))
    }
    agedBoxes.foreach(_.bytes.length shouldBe 1718)
    val firstPayable = (2 to StorageRentClaimBuilder.MaxClaims).find { count =>
      StorageRentClaimBuilder.buildClaim(agedBoxes.take(count), height, dustParams,
        defaultMinerPk, None, Set.empty[ModifierId]).nonEmpty
    }.get
    // The candidate splitter visits 100, 50, 25, then 12/13. A claim of 24
    // clears the fee dust floor, while its next dyadic group can exceed the cap.
    firstPayable shouldBe 24
    val payableClaim = StorageRentClaimBuilder.buildClaim(agedBoxes.take(firstPayable),
      height, dustParams, defaultMinerPk, None, Set.empty[ModifierId]).get
    val dyadicClaim = StorageRentClaimBuilder.buildClaim(agedBoxes.take(25),
      height, dustParams, defaultMinerPk, None, Set.empty[ModifierId]).get
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val costContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, dustParams, validationSettingsNoIl,
      VotingData.empty)(baseSettings.chainSettings).simplifiedUpcoming()
    costContext.currentHeight shouldBe height
    val verifier = ErgoInterpreter(costContext.currentParameters)
    val payableCost = payableClaim.statefulValidity(agedBoxes.take(firstPayable).toIndexedSeq,
      IndexedSeq.empty, costContext)(verifier).get
    val dyadicCost = dyadicClaim.statefulValidity(agedBoxes.take(25).toIndexedSeq,
      IndexedSeq.empty, costContext)(verifier).get
    dyadicCost should be > payableCost
    val limitedParams = Parameters(dustParams.height,
      dustParams.parametersTable.updated(Parameters.MaxBlockCostIncrease, dyadicCost),
      dustParams.proposedUpdate)
    limitedParams.maxBlockCost should be >= (16 * 1024)
    payableCost should be < limitedParams.maxBlockCost
    val limitedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, limitedParams, validationSettingsNoIl,
      VotingData.empty)(baseSettings.chainSettings).simplifiedUpcoming()
    limitedContext.currentParameters.maxBlockCost shouldBe dyadicCost
    payableClaim.statefulValidity(agedBoxes.take(firstPayable).toIndexedSeq,
      IndexedSeq.empty, limitedContext)(ErgoInterpreter(limitedContext.currentParameters))
      .isSuccess shouldBe true
    StorageRentClaimBuilder.buildClaim(agedBoxes.take(12), height, limitedParams,
      defaultMinerPk, None, Set.empty[ModifierId]) shouldBe None
    StorageRentClaimBuilder.buildClaim(agedBoxes.slice(12, 25), height, limitedParams,
      defaultMinerPk, None, Set.empty[ModifierId]) shouldBe None

    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(agedBoxes), None,
      createTempDir, fixtureSettings, limitedParams)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, limitedParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    agedBoxes.zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
        box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
          StorageRentBox(indexed)))
    }
    val history = historyProxy(storage, parentBlock)

    try {
      val agedIds = agedBoxes.map(box => bytesToId(box.id)).toSet
      var scan = CandidateGenerator.RentScanState()
      var offered = Set.empty[ModifierId]
      var sawPayableSplit = false
      (1 to 40).foreach { _ =>
        val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(fixtureSettings), defaultMinerPk,
          Seq.empty, None, fixtureSettings, scan)
        offered ++= attempt.flatMap(_.toOption).toSeq.flatMap(_._1.candidateBlock.transactions)
          .flatMap(_.inputs).map(in => bytesToId(in.boxId)).filter(agedIds.contains)
        val queued = progress.state.pendingClaims.flatten
        queued.size should be <= StorageRentClaimBuilder.MaxClaims
        queued.distinct.size shouldBe queued.size
        sawPayableSplit = sawPayableSplit || progress.state.pendingClaims
          .sliding(2).exists(groups => groups.map(_.size) == Seq(24, 1))
        scan = progress.state
      }
      sawPayableSplit shouldBe true
      withClue(s"claim24Cost=$payableCost claim25Cost=$dyadicCost " +
        s"maxBlockCost=${limitedParams.maxBlockCost} firstPayable=$firstPayable: ") {
        offered should not be empty
      }
      storage.storageRentBoxesAtOrBefore(creationHeight, 100).length shouldBe agedBoxes.size
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  property("over-size rent claim does not block a valid ordinary pool transaction") {
    val height = 3 * Constants.StoragePeriod
    val limitedParams = Parameters(parameters.height,
      parameters.parametersTable.updated(Parameters.MaxBlockSizeIncrease,
        Parameters.MaxBlockSizeMin), parameters.proposedUpdate)
    val agedBoxes = (0 until 12).map { index =>
      testBox(10000000000L + index, Constants.TrueTree,
        height - Constants.StoragePeriod, Seq.empty,
        Map(ErgoBox.R4 -> ByteArrayConstant(Array.fill[Byte](1450)(index.toByte))))
    }
    val witnessBox = testBox(10000000000L, Constants.TrueTree,
      height - 2, Seq.empty, Map.empty)
    val witness = ErgoTransaction(
      IndexedSeq(Input(witnessBox.id, emptyProverResult)),
      IndexedSeq.empty,
      IndexedSeq(new ErgoBoxCandidate(witnessBox.value, Constants.TrueTree, height)))
    val fixtureSettings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true, storageRentCollection = true))
    val baseState = UtxoState.fromBoxHolder(BoxHolder(agedBoxes :+ witnessBox), None,
      createTempDir, fixtureSettings, limitedParams)
    val block = invalidErgoFullBlockGen.sample.get
    val parentHeader = block.header.copy(height = height - 1)
    val parentBlock = block.copy(parentHeader)
    val agedContext = new ErgoStateContext(Seq(parentHeader), None,
      genesisStateDigest, limitedParams, validationSettingsNoIl,
      VotingData.empty)(fixtureSettings.chainSettings)
    val state = new UtxoState(baseState.persistentProver, baseState.version,
      baseState.store, fixtureSettings) {
      override def stateContext: ErgoStateContext = agedContext
      override def emissionBoxOpt: Option[ErgoBox] = None
    }
    val storage = HistoryStorage(fixtureSettings)
    agedBoxes.zipWithIndex.foreach { case (box, index) =>
      val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
        box, index.toLong)
      storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
        Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
          StorageRentBox(indexed)))
    }
    val history = historyProxy(storage, parentBlock)

    try {
      val claim = StorageRentClaimBuilder.buildClaim(agedBoxes, height, limitedParams,
        defaultMinerPk, None, Set.empty[ModifierId]).get
      claim.size should be > limitedParams.maxBlockSize
      val pool = ErgoMemPool.empty(fixtureSettings)
        .put(UnconfirmedTransaction(witness, None))
      val noRentSettings = fixtureSettings.copy(nodeSettings = fixtureSettings.nodeSettings
        .copy(storageRentCollection = false))
      val (witnessOnly, _) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, pool, defaultMinerPk, Seq.empty, None,
        noRentSettings, CandidateGenerator.RentScanState())
      witnessOnly.get.isSuccess shouldBe true
      witnessOnly.get.get._1.candidateBlock.transactions.map(_.id) should contain(witness.id)
      val (ordinaryOverflow, _) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, pool, defaultMinerPk, Seq(claim), None,
        noRentSettings, CandidateGenerator.RentScanState())
      ordinaryOverflow.get.isFailure shouldBe true
      val (attempt, progress) = CandidateGenerator.generateCandidateWithRentScan(
        history, state, pool, defaultMinerPk, Seq.empty, None,
        fixtureSettings, CandidateGenerator.RentScanState())
      attempt.get.isSuccess shouldBe true
      attempt.get.get._1.candidateBlock.transactions.map(_.id) should contain(witness.id)

      val agedIds = agedBoxes.map(box => bytesToId(box.id)).toSet
      var scan = progress.state
      var includedAgedBox = false
      (1 to 6).foreach { _ =>
        val (next, nextProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, pool, defaultMinerPk, Seq.empty, None,
          fixtureSettings, scan)
        next.get.isSuccess shouldBe true
        includedAgedBox = includedAgedBox || next.get.get._1.candidateBlock.transactions.exists { tx =>
          tx.inputs.exists(in => agedIds.contains(bytesToId(in.boxId)))
        }
        nextProgress.state.pendingClaims.flatten.size should be <= StorageRentClaimBuilder.MaxClaims
        scan = nextProgress.state
      }
      includedAgedBox shouldBe true
    } finally {
      storage.close()
      state.closeStorage()
    }
  }

  private def historyProxy(storage: HistoryStorage,
                           parentBlock: org.ergoplatform.modifiers.ErgoFullBlock)
      : ErgoHistoryReader =
    Proxy.newProxyInstance(
      classOf[ErgoHistoryReader].getClassLoader,
      Array[Class[_]](classOf[ErgoHistoryReader]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "bestFullBlockOpt" => Some(parentBlock)
            case "isInSelectedFullChain" =>
              java.lang.Boolean.valueOf(args(0) == parentBlock.id)
            case "storageRentBoxesAtOrBefore" =>
              storage.storageRentBoxesAtOrBefore(
                args(0).asInstanceOf[Int], args(1).asInstanceOf[Int])
            case "storageRentBoxesPage" =>
              storage.storageRentBoxesPage(args(0).asInstanceOf[Int],
                args(1).asInstanceOf[Int], args(2).asInstanceOf[Option[Vector[Byte]]])
            case "removeStorageRentBoxes" =>
              storage.removeStorageRentBoxes(args(0).asInstanceOf[Seq[ModifierId]])
              ().asInstanceOf[AnyRef]
            case "typedExtraIndexById" =>
              storage.getExtraIndex(args(0).asInstanceOf[ModifierId])
            case "typedModifierById" => None
            case "requiredDifficultyAfter" => parentBlock.header.requiredDifficulty
            case other => throw new UnsupportedOperationException(s"unexpected history read: $other")
          }
      }
    ).asInstanceOf[ErgoHistoryReader]
}
