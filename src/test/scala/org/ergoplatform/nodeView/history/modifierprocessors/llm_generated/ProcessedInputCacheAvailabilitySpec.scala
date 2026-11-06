package org.ergoplatform.nodeView.history.modifierprocessors.llm_generated

import com.google.common.cache.Cache
import com.google.common.io.Files.createTempDir
import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.mining.InputBlockFields
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.history.ErgoHistory
import org.ergoplatform.nodeView.state.{BoxHolder, StateType, UtxoState}
import org.ergoplatform.settings.Algos
import org.ergoplatform.subblocks.InputBlockAnnouncement
import org.ergoplatform.utils.{ErgoCompilerHelpers, ErgoCorePropertyTest}
import org.ergoplatform.utils.ErgoCoreTestConstants.parameters
import org.ergoplatform.utils.HistoryTestHelpers.generateHistory
import org.ergoplatform.utils.generators.ChainGenerator.{applyChain, genChain}
import scorex.crypto.authds.ADDigest
import scorex.util.{bytesToId, ModifierId}
import sigma.Colls
import sigma.ast.ErgoTree
import sigma.data.TrivialProp.TrueProp
import sigma.interpreter.ProverResult

class ProcessedInputCacheAvailabilitySpec extends ErgoCorePropertyTest with ErgoCompilerHelpers {
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  private val eb1 = new ErgoBox(
    value = 1000000000L,
    ergoTree = ErgoTree.fromProposition(TrueProp),
    creationHeight = 0,
    additionalTokens = Colls.emptyColl,
    additionalRegisters = Map.empty,
    transactionId = bytesToId(Algos.hash("input-cache-box-1")),
    index = 0
  )

  private val eb2 = new ErgoBox(
    value = 1000000000L,
    ergoTree = compileSourceV5("CONTEXT.minerPubKey.size >= 0", 0),
    creationHeight = 0,
    additionalTokens = Colls.emptyColl,
    additionalRegisters = Map.empty,
    transactionId = bytesToId(Algos.hash("input-cache-box-2")),
    index = 1
  )

  private def digestAfter(txs: Seq[ErgoTransaction], us: UtxoState): ADDigest =
    us.proofsForTransactions(txs).get._2

  private def withInputBlockFixture(test: (UtxoState, ErgoHistory, Header) => Unit): Unit = {
    val us = UtxoState.fromBoxHolder(BoxHolder(Seq(eb1, eb2)), None, createTempDir(), settings, parameters)
    try {
      val h = generateHistory(verifyTransactions = true, StateType.Utxo, PoPoWBootstrap = false, blocksToKeep = -1,
        epochLength = 10000, useLastEpochs = 3, initialDiffOpt = None, None)
      try {
        val orderingChain = genChain(height = 2, history = h, stateOpt = Some(us)).toList
        applyChain(h, orderingChain)
        h.bestFullBlockOpt.get.id shouldBe orderingChain.last.id
        test(us, h, h.bestHeaderOpt.get)
      } finally h.closeStorage()
    } finally us.closeStorage()
  }

  private def announceOne(us: UtxoState, h: ErgoHistory,
                          orderingParent: Header): (InputBlockAnnouncement, ErgoTransaction) = {
    val output = new ErgoBoxCandidate(
      eb1.value,
      eb1.ergoTree,
      0,
      eb1.additionalTokens,
      eb1.additionalRegisters
    )
    val tx = new ErgoTransaction(
      IndexedSeq(new Input(eb1.id, ProverResult.empty)),
      IndexedSeq.empty,
      IndexedSeq(output)
    )
    val header = genChain(2, h, stateOpt = Some(us)).tail.head.header
    header.parentId shouldBe orderingParent.id
    val ib = InputBlockAnnouncement(
      1,
      header.copy(stateRoot = digestAfter(Seq(tx), us)),
      InputBlockFields.empty,
      None
    )
    h.applyInputBlock(ib) shouldBe None
    ib -> tx
  }

  private def transactionsCache(h: ErgoHistory): Cache[ModifierId, ErgoTransaction] = {
    val fields = Iterator.iterate[Class[_]](h.getClass)(_.getSuperclass)
      .takeWhile(_ != null)
      .flatMap(_.getDeclaredFields.iterator)
      .filter { field =>
        field.getName.contains("transactionsCache") &&
          classOf[Cache[_, _]].isAssignableFrom(field.getType)
      }.toVector
    withClue("INPUT_CACHE_FIELD: ") { fields.size shouldBe 1 }
    val field = fields.head
    field.setAccessible(true)
    field.get(h).asInstanceOf[Cache[ModifierId, ErgoTransaction]]
  }

  property("processed input body cache loss is distinct from an unprocessed input block") {
    withInputBlockFixture { (us, h, orderingParent) =>
      val (ib, _) = announceOne(us, h, orderingParent)
      h.getInputBlockTransactionIds(ib.id) shouldBe None
      val tree = h.inputBlocksTree().get
      tree.forks.size shouldBe 1
      tree.bestDepth shouldBe -1
      tree.forks.head.processedIndex shouldBe -1
      h.getBestOrderingCollectedInputBlocksTransactions() shouldBe Seq.empty
    }

    withInputBlockFixture { (us, h, orderingParent) =>
      val (ib, tx) = announceOne(us, h, orderingParent)
      h.applyInputBlockTransactions(ib.id, Seq(tx), us) shouldBe (Seq(ib.id) -> Seq.empty)
      h.getInputBlockTransactionIds(ib.id) shouldBe Some(Seq(tx.id))
      h.bestInputBlocksChain() shouldBe Seq(ib.id)
      val tree = h.inputBlocksTree().get
      tree.forks.size shouldBe 1
      tree.bestDepth shouldBe 0
      tree.forks.head.processedIndex shouldBe 0
      h.getInputBlockTransactions(ib.id) shouldBe Some(Seq(tx))
      h.getBestOrderingCollectedInputBlocksTransactions().map(_.id) shouldBe Seq(tx.id)
    }

    withInputBlockFixture { (us, h, orderingParent) =>
      val (ib, tx) = announceOne(us, h, orderingParent)
      h.applyInputBlockTransactions(ib.id, Seq(tx), us) shouldBe (Seq(ib.id) -> Seq.empty)
      val orderingId = h.bestFullBlockOpt.get.id
      val treeBefore = h.inputBlocksTree().get
      treeBefore.forks.size shouldBe 1
      treeBefore.bestDepth shouldBe 0
      treeBefore.forks.head.processedIndex shouldBe 0
      treeBefore.bestChain shouldBe Seq(ib.id)
      h.getInputBlockTransactionIds(ib.id) shouldBe Some(Seq(tx.id))
      h.getInputBlockTransactions(ib.id) shouldBe Some(Seq(tx))

      val cache = transactionsCache(h)
      Option(cache.getIfPresent(tx.id)).map(_.id) shouldBe Some(tx.id)
      cache.invalidate(tx.id)
      cache.getIfPresent(tx.id) shouldBe null

      h.bestFullBlockOpt.get.id shouldBe orderingId
      h.getInputBlockTransactionIds(ib.id) shouldBe Some(Seq(tx.id))
      val treeAfter = h.inputBlocksTree().get
      treeAfter.bestChain shouldBe treeBefore.bestChain
      treeAfter.bestDepth shouldBe treeBefore.bestDepth
      treeAfter.forks.head.processedIndex shouldBe 0
      h.getInputBlockTransactions(ib.id) shouldBe Some(Seq.empty)
      withClue("PROCESSED_INPUT_BODY_CACHE_LOSS: ") {
        h.getBestOrderingCollectedInputBlocksTransactions() shouldBe Seq.empty
      }
    }
  }
}
