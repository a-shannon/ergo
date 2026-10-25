package org.ergoplatform.nodeView.mempool

import org.ergoplatform.ErgoBox.BoxId
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.settings.Constants.TrueTree
import org.ergoplatform.utils.ErgoTestHelpers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.crypto.authds.ADKey
import scorex.util.ModifierId

class OrderedTxPoolSpec extends AnyFlatSpec with Matchers with ErgoTestHelpers {
  import org.ergoplatform.utils.ErgoCoreTestConstants.emptyProverResult
  import org.ergoplatform.utils.ErgoNodeTestConstants.settings

  private final class ReconvergentFixture(
    val a: ErgoTransaction,
    val b: ErgoTransaction,
    val c: ErgoTransaction,
    val d: ErgoTransaction,
    val beforeD: ErgoMemPool,
    val parentOrder: Seq[ModifierId]
  )

  private def deterministicRootId(nonce: Int): BoxId = {
    val bytes = Array.fill[Byte](32)(0)
    bytes(28) = (nonce >>> 24).toByte
    bytes(29) = (nonce >>> 16).toByte
    bytes(30) = (nonce >>> 8).toByte
    bytes(31) = nonce.toByte
    ADKey @@ bytes
  }

  private def outputCandidates(plainCount: Int, plainValue: Long = 4000000L) = {
    val feeProposition = settings.chainSettings.monetary.feeProposition
    IndexedSeq.fill(plainCount)(new org.ergoplatform.ErgoBoxCandidate(plainValue, TrueTree, 0)) :+
      new org.ergoplatform.ErgoBoxCandidate(1000000L, feeProposition, 0)
  }

  private def buildReconvergentFixture(nonce: Int): ReconvergentFixture = {
    val a = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(deterministicRootId(nonce), emptyProverResult)),
      outputCandidates(3)
    )
    val b = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(a.outputs(0).id, emptyProverResult)),
      outputCandidates(1, 3000000L)
    )
    val c = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(a.outputs(1).id, emptyProverResult)),
      outputCandidates(1, 3000000L)
    )
    val d = ErgoTransaction(
      IndexedSeq(
        new org.ergoplatform.Input(b.outputs(0).id, emptyProverResult),
        new org.ergoplatform.Input(c.outputs(0).id, emptyProverResult),
        new org.ergoplatform.Input(a.outputs(2).id, emptyProverResult)
      ),
      outputCandidates(1, 9000000L)
    )

    val beforeD = Seq(a, b, c).foldLeft(ErgoMemPool.empty(settings)) {
      case (pool, tx) => pool.put(UnconfirmedTransaction(tx, None))
    }
    val uniqueParentKeys = d.inputs.flatMap(input => beforeD.pool.outputs.get(input.boxId)).toSet
    val parentOrder = uniqueParentKeys
      .flatMap { wtx =>
        beforeD.pool.orderedTransactions.get(wtx).map(unconfirmed => wtx -> unconfirmed)
      }
      .toSeq
      .map(_._1.id)

    new ReconvergentFixture(a, b, c, d, beforeD, parentOrder)
  }

  private def fixtureWithSharedAncestorLast(): ReconvergentFixture = {
    val fixture = (0 until 4096)
      .iterator
      .map(buildReconvergentFixture)
      .find { candidate =>
        candidate.parentOrder.lastOption.contains(candidate.a.id) &&
          candidate.parentOrder.take(2).toSet == Set(candidate.b.id, candidate.c.id)
      }
    fixture.getOrElse(fail("No deterministic reconvergent fixture found"))
  }

  private def orderedIds(pool: OrderedTxPool): Vector[ModifierId] =
    pool.orderedTransactions.valuesIterator.map(_.id).toVector

  private def assertConsistent(pool: OrderedTxPool, expectedIds: Set[ModifierId]): Unit = {
    val ids = orderedIds(pool)
    ids.toSet shouldBe expectedIds
    ids.distinct.size shouldBe ids.size
    pool.orderedTransactions.size shouldBe expectedIds.size
    pool.transactionsRegistry.keySet shouldBe expectedIds
    pool.transactionsRegistry.foreach { case (id, key) =>
      pool.orderedTransactions.get(key).map(_.id) shouldBe Some(id)
    }
    val transactions = pool.orderedTransactions.valuesIterator.map(_.transaction).toVector
    val expectedOutputIds = transactions.flatMap(_.outputs.map(_.id)).toSet
    val expectedInputIds = transactions.flatMap(_.inputs.map(_.boxId)).toSet
    pool.outputs.keySet shouldBe expectedOutputIds
    pool.inputs.keySet shouldBe expectedInputIds
    pool.orderedTransactions.foreach { case (key, unconfirmed) =>
      unconfirmed.transaction.outputs.foreach { output =>
        pool.outputs.get(output.id) shouldBe Some(key)
      }
      unconfirmed.transaction.inputs.foreach { input =>
        pool.inputs.get(input.boxId) shouldBe Some(key)
      }
    }
    pool.outputs.valuesIterator.foreach { key =>
      pool.orderedTransactions.contains(key) shouldBe true
    }
    pool.inputs.valuesIterator.foreach { key =>
      pool.orderedTransactions.contains(key) shouldBe true
    }
  }

  private def withDuplicate(pool: OrderedTxPool, tx: ErgoTransaction): OrderedTxPool = {
    val registeredKey = pool.transactionsRegistry(tx.id)
    val unconfirmed = pool.orderedTransactions(registeredKey)
    val duplicateKey = registeredKey.copy(weight = registeredKey.weight + 1L)

    new OrderedTxPool(
      pool.orderedTransactions.updated(duplicateKey, unconfirmed),
      pool.transactionsRegistry,
      pool.invalidatedTxIds,
      pool.outputs,
      pool.inputs
    )(settings)
  }

  private def withoutRegistry(pool: OrderedTxPool, tx: ErgoTransaction): OrderedTxPool = {
    new OrderedTxPool(
      pool.orderedTransactions,
      pool.transactionsRegistry - tx.id,
      pool.invalidatedTxIds,
      pool.outputs,
      pool.inputs
    )(settings)
  }

  it should "refresh fee-per-cost priority when a rechecked transaction changes cost" in {
    implicit val monetarySettings = settings.chainSettings.monetary

    def transaction(nonce: Int): ErgoTransaction = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(deterministicRootId(nonce), emptyProverResult)),
      outputCandidates(1)
    )

    val tx = transaction(9001)
    val otherTx = transaction(9002)
    val firstCost = 1000
    val otherCost = 1500
    val recheckedCost = 2000
    val initial = UnconfirmedTransaction(tx, None).withCost(firstCost)
    val rechecked = initial.withCost(recheckedCost)
    val other = UnconfirmedTransaction(otherTx, None).withCost(otherCost)

    val before = OrderedTxPool.empty(settings)
      .put(initial, firstCost)
      .put(other, otherCost)
    before.orderedTransactions.firstKey.id shouldBe tx.id

    val after = before.put(rechecked, recheckedCost)
    after.get(tx.id).flatMap(_.lastCost) shouldBe Some(recheckedCost)
    val freshWeight = OrderedTxPool.weighted(tx, recheckedCost).weight
    freshWeight should be < before.transactionsRegistry(tx.id).weight
    after.transactionsRegistry(tx.id).weight shouldBe freshWeight
    after.orderedTransactions.firstKey.id shouldBe otherTx.id

    val sizeFactor = tx.size
    val bySizeBefore = OrderedTxPool.empty(settings).put(initial, sizeFactor)
    val bySizeAfter = bySizeBefore.put(rechecked, sizeFactor)
    bySizeAfter.get(tx.id).flatMap(_.lastCost) shouldBe Some(recheckedCost)
    bySizeAfter.transactionsRegistry(tx.id).weight shouldBe
      bySizeBefore.transactionsRegistry(tx.id).weight
  }

  it should "propagate only the changed base weight through a rechecked family" in {
    implicit val monetarySettings = settings.chainSettings.monetary

    val parent = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(deterministicRootId(9003), emptyProverResult)),
      outputCandidates(1)
    )
    val child = ErgoTransaction(
      IndexedSeq(new org.ergoplatform.Input(parent.outputs.head.id, emptyProverResult)),
      outputCandidates(1, 3000000L)
    )
    val parentUtx = UnconfirmedTransaction(parent, None).withCost(1000)
    val childUtx = UnconfirmedTransaction(child, None).withCost(1000)
    val before = OrderedTxPool.empty(settings).put(parentUtx, 1000).put(childUtx, 1000)
    val parentBefore = before.transactionsRegistry(parent.id)
    val childBefore = before.transactionsRegistry(child.id)
    assertConsistent(before, Set(parent.id, child.id))

    val childAfter = before.put(childUtx.withCost(2000), 2000)
    val childBaseAfter = OrderedTxPool.weighted(child, 2000).feePerFactor
    val childDelta = childBaseAfter - childBefore.feePerFactor
    childAfter.transactionsRegistry(child.id).weight shouldBe childBaseAfter
    childAfter.transactionsRegistry(child.id).feePerFactor shouldBe childBaseAfter
    childAfter.transactionsRegistry(child.id).created shouldBe childBefore.created
    childAfter.transactionsRegistry(parent.id).weight shouldBe parentBefore.weight + childDelta
    childAfter.transactionsRegistry(parent.id).feePerFactor shouldBe parentBefore.feePerFactor
    childAfter.transactionsRegistry(parent.id).created shouldBe parentBefore.created
    assertConsistent(childAfter, Set(parent.id, child.id))

    val parentAfter = childAfter.put(parentUtx.withCost(2000), 2000)
    val parentBaseAfter = OrderedTxPool.weighted(parent, 2000).feePerFactor
    val parentDelta = parentBaseAfter - parentBefore.feePerFactor
    parentAfter.transactionsRegistry(parent.id).weight shouldBe
      childAfter.transactionsRegistry(parent.id).weight + parentDelta
    parentAfter.transactionsRegistry(parent.id).feePerFactor shouldBe parentBaseAfter
    parentAfter.transactionsRegistry(parent.id).created shouldBe parentBefore.created
    parentAfter.transactionsRegistry(child.id).weight shouldBe childBaseAfter
    parentAfter.transactionsRegistry(child.id).created shouldBe childBefore.created
    assertConsistent(parentAfter, Set(parent.id, child.id))

    val unchanged = parentAfter.put(parentUtx.withCost(2000), 2000)
    unchanged.transactionsRegistry(parent.id).weight shouldBe parentAfter.transactionsRegistry(parent.id).weight
    assertConsistent(unchanged, Set(parent.id, child.id))
  }

  it should "keep indexes consistent when a transaction closes a reconvergent family" in {
    val fixture = fixtureWithSharedAncestorLast()
    val beforeIds = Set(fixture.a.id, fixture.b.id, fixture.c.id)
    val beforeWeights = beforeIds.map { id =>
      id -> fixture.beforeD.pool.transactionsRegistry(id).weight
    }.toMap

    fixture.parentOrder.last shouldBe fixture.a.id
    fixture.parentOrder.take(2).toSet shouldBe Set(fixture.b.id, fixture.c.id)
    fixture.d.inputs.map(_.boxId).distinct.size shouldBe fixture.d.inputs.size
    assertConsistent(fixture.beforeD.pool, beforeIds)

    val afterD = fixture.beforeD.put(UnconfirmedTransaction(fixture.d, None))
    val expectedIds = beforeIds + fixture.d.id
    val duplicateCounts = orderedIds(afterD.pool).groupBy(identity).mapValues(_.size)
    val orderedKeys = afterD.pool.orderedTransactions.keysIterator
      .map(key => key.id -> key.weight)
      .toVector

    withClue(s"ordered keys=$orderedKeys, duplicate counts=$duplicateCounts") {
      assertConsistent(afterD.pool, expectedIds)
      val afterWeights = afterD.pool.transactionsRegistry.mapValues(_.weight)
      val dWeight = afterWeights(fixture.d.id)
      dWeight should be > (0L)
      afterWeights(fixture.b.id) shouldBe beforeWeights(fixture.b.id) + dWeight
      afterWeights(fixture.c.id) shouldBe beforeWeights(fixture.c.id) + dWeight
      afterWeights(fixture.a.id) shouldBe beforeWeights(fixture.a.id) + 3L * dWeight
    }
  }


  it should "heal a duplicated ancestor while propagating a new child" in {
    val fixture = fixtureWithSharedAncestorLast()
    val before = fixture.beforeD.pool
    val ancestorWeight = before.transactionsRegistry(fixture.a.id).weight
    val corrupted = withDuplicate(before, fixture.a)

    orderedIds(corrupted).count(_ == fixture.a.id) shouldBe 2
    val healed = corrupted.put(UnconfirmedTransaction(fixture.d, None), fixture.d.size)

    assertConsistent(
      healed,
      Set(fixture.a.id, fixture.b.id, fixture.c.id, fixture.d.id)
    )
    val dWeight = healed.transactionsRegistry(fixture.d.id).weight
    healed.transactionsRegistry(fixture.a.id).weight shouldBe ancestorWeight + 3L * dWeight
  }
  it should "self-heal duplicate keys without propagating family weight twice" in {
    val fixture = fixtureWithSharedAncestorLast()
    val before = fixture.beforeD.pool
    val originalKey = before.transactionsRegistry(fixture.b.id)
    val ancestorWeight = before.transactionsRegistry(fixture.a.id).weight
    val corrupted = withDuplicate(before, fixture.b)

    orderedIds(corrupted).count(_ == fixture.b.id) shouldBe 2
    val healed = corrupted.put(UnconfirmedTransaction(fixture.b, None), fixture.b.size)

    assertConsistent(healed, Set(fixture.a.id, fixture.b.id, fixture.c.id))
    val healedKeys = healed.orderedTransactions.keysIterator
      .filter(_.id == fixture.b.id)
      .toVector
    healedKeys.map(_.weight) shouldBe Vector(originalKey.weight)
    healedKeys.map(_.created) shouldBe Vector(originalKey.created)
    healed.transactionsRegistry(fixture.a.id).weight shouldBe ancestorWeight
  }

  it should "purge duplicate keys and subtract family weight once on removal" in {
    val fixture = fixtureWithSharedAncestorLast()
    val before = fixture.beforeD.pool
    val ancestorWeight = before.transactionsRegistry(fixture.a.id).weight
    val childWeight = before.transactionsRegistry(fixture.b.id).weight
    val siblingWeight = before.transactionsRegistry(fixture.c.id).weight
    val corrupted = withDuplicate(before, fixture.b)

    orderedIds(corrupted).count(_ == fixture.b.id) shouldBe 2
    val removed = corrupted.remove(fixture.b)

    assertConsistent(removed, Set(fixture.a.id, fixture.c.id))
    removed.transactionsRegistry(fixture.a.id).weight shouldBe ancestorWeight - childWeight
    removed.transactionsRegistry(fixture.c.id).weight shouldBe siblingWeight
  }

  it should "purge duplicate keys and subtract family weight once on invalidation" in {
    val fixture = fixtureWithSharedAncestorLast()
    val before = fixture.beforeD.pool
    val ancestorWeight = before.transactionsRegistry(fixture.a.id).weight
    val childWeight = before.transactionsRegistry(fixture.b.id).weight
    val siblingWeight = before.transactionsRegistry(fixture.c.id).weight
    val corrupted = withDuplicate(before, fixture.b)

    orderedIds(corrupted).count(_ == fixture.b.id) shouldBe 2
    val invalidated = corrupted.invalidate(UnconfirmedTransaction(fixture.b, None))

    assertConsistent(invalidated, Set(fixture.a.id, fixture.c.id))
    invalidated.transactionsRegistry(fixture.a.id).weight shouldBe ancestorWeight - childWeight
    invalidated.transactionsRegistry(fixture.c.id).weight shouldBe siblingWeight
    invalidated.isInvalidated(fixture.b.id) shouldBe true
  }

  it should "purge unregistered orphan keys on removal and invalidation" in {
    val fixture = fixtureWithSharedAncestorLast()
    val before = fixture.beforeD.pool
    val orphaned = withoutRegistry(before, fixture.b)
    val expectedIds = Set(fixture.a.id, fixture.c.id)

    orphaned.transactionsRegistry should not contain fixture.b.id
    orderedIds(orphaned).count(_ == fixture.b.id) shouldBe 1

    val removed = orphaned.remove(fixture.b)
    assertConsistent(removed, expectedIds)

    val invalidated = orphaned.invalidate(UnconfirmedTransaction(fixture.b, None))
    assertConsistent(invalidated, expectedIds)
    invalidated.isInvalidated(fixture.b.id) shouldBe true
  }
}
