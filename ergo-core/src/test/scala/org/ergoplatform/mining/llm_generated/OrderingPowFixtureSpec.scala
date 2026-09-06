package org.ergoplatform.mining.llm_generated

import com.google.common.primitives.Longs
import org.ergoplatform.{AutolykosSolution, BlockSolutionSearchResult, InputSolutionFound, NoSolutionFound, OrderingSolutionFound}
import org.ergoplatform.mining.AutolykosV1SolutionSerializer
import org.ergoplatform.utils.ErgoCorePropertyTest
import sigma.crypto.CryptoConstants

/** Deterministic search outcomes exercise fixture control flow without computing PoW. */
class OrderingPowFixtureSpec extends ErgoCorePropertyTest {
  private def solution(nonce: Long): AutolykosSolution =
    new AutolykosSolution(CryptoConstants.dlogGroup.generator, CryptoConstants.dlogGroup.generator,
      Longs.toByteArray(nonce), BigInt(1))

  private class Script(expected: Vector[(Long, Long, BlockSolutionSearchResult)]) {
    private var position = 0

    def search(start: Long, end: Long): BlockSolutionSearchResult = {
      withClue("unexpected search callback: ") { position should be < expected.size }
      val (expectedStart, expectedEnd, outcome) = expected(position)
      (start, end) shouldBe ((expectedStart, expectedEnd))
      position += 1
      outcome
    }

    def assertComplete(): Unit = position shouldBe expected.size
  }

  property("an immediately accepted ordering solution is returned unchanged") {
    val selected = solution(12L)
    val bytes = new AutolykosV1SolutionSerializer().toBytes(selected)
    val script = new Script(Vector((10L, 20L, OrderingSolutionFound(selected))))
    var accepted = Vector.empty[Long]
    val result = OrderingPowFixture.find(10L, 20L, candidate => {
      accepted :+= Longs.fromByteArray(candidate.n)
      true
    })(script.search)
    result.isDefined shouldBe true
    (result.get.asInstanceOf[AnyRef] eq selected.asInstanceOf[AnyRef]) shouldBe true
    new AutolykosV1SolutionSerializer().toBytes(result.get) shouldBe bytes
    accepted shouldBe Vector(12L)
    script.assertComplete()
  }

  property("input solutions advance the cursor while preserving the exclusive end") {
    val selected = solution(18L)
    val script = new Script(Vector((10L, 20L, InputSolutionFound(solution(12L))),
      (13L, 20L, InputSolutionFound(solution(15L))), (16L, 20L, OrderingSolutionFound(selected))))
    var accepted = Vector.empty[Long]
    val result = OrderingPowFixture.find(10L, 20L, candidate => {
      accepted :+= Longs.fromByteArray(candidate.n)
      true
    })(script.search)
    result shouldBe Some(selected)
    accepted shouldBe Vector(18L)
    script.assertComplete()
  }

  property("an unselected ordering solution advances before the next predicate call") {
    val selected = solution(17L)
    val script = new Script(Vector((10L, 20L, OrderingSolutionFound(solution(13L))),
      (14L, 20L, OrderingSolutionFound(selected))))
    var accepted = Vector.empty[Long]
    val result = OrderingPowFixture.find(10L, 20L, candidate => {
      val nonce = Longs.fromByteArray(candidate.n)
      accepted :+= nonce
      nonce == 17L
    })(script.search)
    result shouldBe Some(selected)
    accepted shouldBe Vector(13L, 17L)
    script.assertComplete()
  }

  property("NoSolutionFound stops without applying the selection predicate") {
    val script = new Script(Vector((10L, 20L, NoSolutionFound)))
    val result = OrderingPowFixture.find(10L, 20L, _ => fail("unexpected selection predicate"))(script.search)
    result shouldBe None
    script.assertComplete()
  }

  property("empty and exhausted ranges do not call search or the predicate") {
    Seq((20L, 20L), (21L, 20L)).foreach { case (start, end) =>
      val script = new Script(Vector.empty)
      val result = OrderingPowFixture.find(start, end, _ => fail("unexpected selection predicate"))(script.search)
      result shouldBe None
      script.assertComplete()
    }
  }

  property("an input solution at the final nonce exhausts the range") {
    val script = new Script(Vector((10L, 20L, InputSolutionFound(solution(19L)))))
    val result = OrderingPowFixture.find(10L, 20L, _ => fail("unexpected selection predicate"))(script.search)
    result shouldBe None
    script.assertComplete()
  }

  property("an input solution followed by NoSolutionFound stops at the remaining range") {
    val script = new Script(Vector((10L, 20L, InputSolutionFound(solution(14L))), (15L, 20L, NoSolutionFound)))
    val result = OrderingPowFixture.find(10L, 20L, _ => fail("unexpected selection predicate"))(script.search)
    result shouldBe None
    script.assertComplete()
  }

  property("an unselected ordering solution at the final nonce exhausts the range") {
    val script = new Script(Vector((10L, 20L, OrderingSolutionFound(solution(19L)))))
    var accepted = Vector.empty[Long]
    val result = OrderingPowFixture.find(10L, 20L, candidate => {
      accepted :+= Longs.fromByteArray(candidate.n)
      false
    })(script.search)
    result shouldBe None
    accepted shouldBe Vector(19L)
    script.assertComplete()
  }
}
