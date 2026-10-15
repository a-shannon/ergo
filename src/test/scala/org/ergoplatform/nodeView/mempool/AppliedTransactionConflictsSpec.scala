package org.ergoplatform.nodeView.mempool

import org.ergoplatform.{DataInput, ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.nodeView.mempool.ErgoMemPoolUtils.ProcessingOutcome
import org.ergoplatform.nodeView.state.BoxHolder
import org.ergoplatform.nodeView.state.wrapped.WrappedUtxoState
import org.ergoplatform.settings.Algos
import org.ergoplatform.settings.Constants.TrueTree
import org.ergoplatform.utils.ErgoTestHelpers
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.crypto.authds.ADKey
import scorex.util.bytesToId
import sigma.Colls

class AppliedTransactionConflictsSpec extends AnyFlatSpec with ErgoTestHelpers with Matchers {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  private def transaction(seeds: Byte*)(fee: Long): ErgoTransaction = {
    ErgoTransaction(
      seeds.map(seed => new Input(ADKey @@ Array.fill(32)(seed), emptyProverResult)).toIndexedSeq,
      IndexedSeq(new ErgoBoxCandidate(fee, feeProp, creationHeight = 0))
    )
  }

  private def poolWith(txs: ErgoTransaction*): ErgoMemPool = {
    val pool = ErgoMemPool.empty(settings).put(txs.map(UnconfirmedTransaction(_, None)))
    val now = System.currentTimeMillis()
    new ErgoMemPool(pool.pool, MemPoolStatistics(now, 7, now), pool.sortingOption)(settings)
  }

  private case class UtxoChainFixture(state: WrappedUtxoState,
                                      genesis: ErgoFullBlock,
                                      parent: ErgoTransaction,
                                      child: ErgoTransaction,
                                      dataChild: ErgoTransaction,
                                      grandchild: ErgoTransaction,
                                      replacement: ErgoTransaction,
                                      unrelated: ErgoTransaction)

  private def spend(box: ErgoBox, fee: Long): ErgoTransaction =
    ErgoTransaction(
      IndexedSeq(new Input(box.id, emptyProverResult)),
      IndexedSeq(
        new ErgoBoxCandidate(box.value - fee, TrueTree, 0, box.additionalTokens),
        new ErgoBoxCandidate(fee, feeProp, 0)
      )
    )

  private def withUtxoChain(test: UtxoChainFixture => Unit): Unit = {
    val initial = new ErgoBox(100000000L, TrueTree, Colls.emptyColl, Map.empty,
      bytesToId(Array.fill(32)(1.toByte)), 0.toShort, creationHeight = 0)
    val boxHolder = BoxHolder(Seq(initial))
    val utxoState = createUtxoState(boxHolder, settings.launchParameters)
    try {
      val funding = ErgoTransaction(
        IndexedSeq(new Input(initial.id, emptyProverResult)),
        IndexedSeq(
          new ErgoBoxCandidate(30000000L, TrueTree, 0),
          new ErgoBoxCandidate(30000000L, TrueTree, 0),
          new ErgoBoxCandidate(39000000L, TrueTree, 0),
          new ErgoBoxCandidate(1000000L, feeProp, 0)
        )
      )
      val genesis = validFullBlock(None, utxoState, Seq(funding))
      val state = WrappedUtxoState(utxoState, boxHolder, settings)
        .applyModifier(genesis)(_ => ()).get
      // A real funding block provides deterministic roots for independent branches and a data provider.
      val roots = funding.outputs.take(3)
      roots.size shouldBe 3
      roots.foreach(box => state.boxById(box.id).isDefined shouldBe true)
      val parent = spend(roots.head, 1000000L)
      val child = spend(parent.outputs.head, 1000000L)
      val dataChild = ErgoTransaction(
        IndexedSeq(new Input(roots(1).id, emptyProverResult)),
        IndexedSeq(DataInput(ADKey @@ parent.outputs.head.id.clone())),
        IndexedSeq(
          new ErgoBoxCandidate(roots(1).value - 1000000L, TrueTree, 0, roots(1).additionalTokens),
          new ErgoBoxCandidate(1000000L, feeProp, 0)
        )
      )
      val grandchild = spend(dataChild.outputs.head, 1000000L)
      val replacement = spend(roots.head, 2000000L)
      val unrelated = spend(roots(2), 1000000L)
      test(UtxoChainFixture(state, genesis, parent, child, dataChild, grandchild, replacement, unrelated))
    } finally utxoState.closeStorage()
  }

  private def putAccepted(pool: ErgoMemPool,
                          tx: ErgoTransaction,
                          state: WrappedUtxoState): ErgoMemPool = {
    val (updated, outcome) = pool.process(UnconfirmedTransaction(tx, None), state)
    outcome.isInstanceOf[ProcessingOutcome.Accepted] shouldBe true
    updated
  }

  private def assertExactPool(pool: ErgoMemPool, expected: Seq[ErgoTransaction]): Unit = {
    val expectedIds = expected.map(_.id).toSet
    val expectedInputs = expected.flatMap(_.inputs.map(input => Algos.encode(input.boxId))).toSet
    val expectedOutputs = expected.flatMap(_.outputs.map(output => Algos.encode(output.id))).toSet
    pool.getAll.map(_.id).toSet shouldBe expectedIds
    pool.pool.orderedTransactions.valuesIterator.map(_.id).toSet shouldBe expectedIds
    pool.pool.transactionsRegistry.keySet shouldBe expectedIds
    pool.spentInputs.map(Algos.encode).toSet shouldBe expectedInputs
    pool.pool.inputs.keysIterator.map(Algos.encode).toSet shouldBe expectedInputs
    pool.pool.outputs.keysIterator.map(Algos.encode).toSet shouldBe expectedOutputs
  }

  it should "remove applied transaction conflicts when the applied transaction is absent" in {
    val applied = transaction(1)(1000000L)
    val conflict = transaction(1)(2000000L)
    val unrelated = transaction(2)(1000000L)
    val before = poolWith(conflict, unrelated)
    before.contains(applied.id) shouldBe false
    before.contains(conflict.id) shouldBe true

    val after = before.removeWithDoubleSpends(Seq(applied))

    after.getAll.map(_.id).toSet shouldBe Set(unrelated.id)
    after.spentInputs.toSet shouldBe unrelated.inputs.map(_.boxId).toSet
    after.pool.outputs.keySet shouldBe unrelated.outputs.map(_.id).toSet
    after.stats.takenTxns shouldBe 8
    before.contains(conflict.id) shouldBe true
  }

  it should "preserve statistics when an absent applied transaction has no conflicts" in {
    val applied = transaction(1)(1000000L)
    val unrelated = transaction(2)(1000000L)
    val before = poolWith(unrelated)

    val after = before.removeWithDoubleSpends(Seq(applied))

    after.getAll shouldBe before.getAll
    after.stats shouldBe before.stats
    before.removeTxAndDoubleSpends(applied).stats shouldBe before.stats
  }

  it should "remove conflicts for every input of an absent applied transaction" in {
    val applied = transaction(1, 2)(1000000L)
    val first = transaction(1)(2000000L)
    val second = transaction(2)(2000000L)
    val unrelated = transaction(3)(1000000L)

    val after = poolWith(first, second, unrelated).removeWithDoubleSpends(Seq(applied))

    after.getAll.map(_.id).toSet shouldBe Set(unrelated.id)
    after.stats.takenTxns shouldBe 9
  }

  it should "count a conflict sharing several inputs only once" in {
    val applied = transaction(1, 2)(1000000L)
    val conflict = transaction(1, 2)(2000000L)

    val after = poolWith(conflict).removeWithDoubleSpends(Seq(applied, applied))

    after.size shouldBe 0
    after.stats.takenTxns shouldBe 8
  }

  it should "remove a present applied transaction once and retain its unconfirmed child" in {
    val applied = transaction(1)(1000000L)
    val child = applied.copy(
      inputs = IndexedSeq(new Input(applied.outputs.head.id, emptyProverResult))
    )
    val unrelated = transaction(2)(1000000L)
    val before = poolWith(applied, child, unrelated)

    val after = before.removeWithDoubleSpends(Seq(applied, applied))

    after.getAll.map(_.id).toSet shouldBe Set(child.id, unrelated.id)
    after.stats.takenTxns shouldBe 8
    after.pool.outputs.keySet shouldBe (child.outputs ++ unrelated.outputs).map(_.id).toSet
  }

  it should "remove an applied transaction retained only in the ordered index" in {
    val applied = transaction(1)(1000000L)
    val unrelated = transaction(2)(1000000L)
    val healthy = poolWith(applied, unrelated)
    val ordered = healthy.pool
    val incomplete = new OrderedTxPool(
      ordered.orderedTransactions,
      ordered.transactionsRegistry - applied.id,
      ordered.invalidatedTxIds,
      ordered.outputs,
      ordered.inputs -- applied.inputs.map(_.boxId)
    )(settings)
    val before = new ErgoMemPool(incomplete, healthy.stats, healthy.sortingOption)(settings)
    before.contains(applied.id) shouldBe false
    before.getAll.map(_.id).toSet shouldBe Set(applied.id, unrelated.id)

    val after = before.removeWithDoubleSpends(Seq(applied))

    after.getAll.map(_.id) shouldBe Seq(unrelated.id)
    after.pool.outputs.keySet shouldBe unrelated.outputs.map(_.id).toSet
    after.stats shouldBe before.stats
  }

  it should "invalidate an absent transaction without changing pool contents or statistics" in {
    val absent = transaction(1)(1000000L)
    val unrelated = transaction(2)(1000000L)
    val before = poolWith(unrelated)
    before.pool.isInvalidated(absent.id) shouldBe false

    val after = before.invalidate(UnconfirmedTransaction(absent, None))

    after.getAll shouldBe before.getAll
    after.stats shouldBe before.stats
    after.pool.isInvalidated(absent.id) shouldBe true
  }

  it should "remove a direct conflict and its orphaned child while retaining an unrelated transaction" in {
    withUtxoChain { f =>
      f.parent.inputs.head.boxId.sameElements(f.replacement.inputs.head.boxId) shouldBe true
      f.child.inputs.head.boxId.sameElements(f.parent.outputs.head.id) shouldBe true
      f.parent.inputs.head.boxId.sameElements(f.unrelated.inputs.head.boxId) shouldBe false
      f.state.boxById(f.parent.inputs.head.boxId).isDefined shouldBe true
      f.state.boxById(f.unrelated.inputs.head.boxId).isDefined shouldBe true

      val before = Seq(f.parent, f.child, f.unrelated)
        .foldLeft(ErgoMemPool.empty(settings)) { case (pool, tx) => putAccepted(pool, tx, f.state) }
      putAccepted(ErgoMemPool.empty(settings), f.replacement, f.state)
      assertExactPool(before, Seq(f.parent, f.child, f.unrelated))

      val after = before.removeWithDoubleSpends(Seq(f.replacement))

      assertExactPool(after, Seq(f.unrelated))
    }
  }

  it should "retain a child made valid by confirmation of its pooled parent" in {
    withUtxoChain { f =>
      val before = Seq(f.parent, f.child, f.unrelated)
        .foldLeft(ErgoMemPool.empty(settings)) { case (pool, tx) => putAccepted(pool, tx, f.state) }

      val after = before.removeWithDoubleSpends(Seq(f.parent))

      assertExactPool(after, Seq(f.child, f.unrelated))
      val parentBlock = validFullBlock(
        Some(f.genesis),
        f.state,
        Seq(f.parent),
        Some(f.genesis.header.timestamp + 1L)
      )
      val postParentState = f.state.applyModifier(parentBlock)(_ => ()).get
      putAccepted(ErgoMemPool.empty(settings), f.child, postParentState)
        .contains(f.child.id) shouldBe true
    }
  }

  it should "remove data-input descendants of a conflicting producer but retain them when it is confirmed" in {
    withUtxoChain { f =>
      f.dataChild.inputs.head.boxId.sameElements(f.parent.inputs.head.boxId) shouldBe false
      f.dataChild.dataInputs.map(input => Algos.encode(input.boxId)).toSeq shouldBe
        Seq(Algos.encode(f.parent.outputs.head.id))
      f.grandchild.inputs.head.boxId.sameElements(f.dataChild.outputs.head.id) shouldBe true

      val transactions = Seq(f.parent, f.dataChild, f.grandchild, f.unrelated)
      val before = transactions.foldLeft(ErgoMemPool.empty(settings)) { case (pool, tx) =>
        putAccepted(pool, tx, f.state)
      }
      assertExactPool(before, transactions)
      before.pool.inputs.contains(f.parent.outputs.head.id) shouldBe false

      val afterConflict = before.removeWithDoubleSpends(Seq(f.replacement))
      assertExactPool(afterConflict, Seq(f.unrelated))

      val afterConfirmation = before.removeWithDoubleSpends(Seq(f.parent))
      assertExactPool(afterConfirmation, Seq(f.dataChild, f.grandchild, f.unrelated))
      val parentBlock = validFullBlock(
        Some(f.genesis),
        f.state,
        Seq(f.parent),
        Some(f.genesis.header.timestamp + 1L)
      )
      val postParentState = f.state.applyModifier(parentBlock)(_ => ()).get
      val withDataChild = putAccepted(ErgoMemPool.empty(settings), f.dataChild, postParentState)
      putAccepted(withDataChild, f.grandchild, postParentState).contains(f.grandchild.id) shouldBe true
    }
  }
}
