package org.ergoplatform.mining

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate}
import org.ergoplatform.settings.{Constants, MainnetLaunchParameters}
import org.ergoplatform.utils.ErgoCoreTestConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.bytesToId
import sigma.Colls

/** Checks the serialized dust boundary for one burned box's miner payout. */
class StorageRentClaimDustBoundarySpec extends AnyFlatSpec with Matchers {
  private val parameters = MainnetLaunchParameters
  private val claimHeight = 3 * Constants.StoragePeriod
  private val dummyTxId = bytesToId(Array.fill(32)(0.toByte))

  private def claimFor(inputValue: Long) = {
    val box: ErgoBox = new ErgoBoxCandidate(inputValue, Constants.TrueTree, 0,
      Colls.emptyColl, Map.empty).toBox(dummyTxId, 0.toShort)
    StorageRentClaimBuilder.buildClaim(Seq(box), claimHeight, parameters,
      ErgoCoreTestConstants.defaultMinerPk, None, Set.empty)
  }

  private def actualDust(output: ErgoBoxCandidate, index: Int): Long =
    output.toBox(dummyTxId, index.toShort).bytes.length.toLong * parameters.minValuePerByte

  "A burned storage-rent box" should "be skipped when its own payout is below dust" in {
    val accepted = claimFor(45000L).getOrElse(fail("expected the control payout"))
    val below = actualDust(accepted.outputCandidates.head, 0) / 2
    claimFor(below) shouldBe None
  }

  it should "retain a payout that clears the actual serialized dust floor" in {
    val tx = claimFor(45000L).getOrElse(fail("expected a claim for 45000 nanoERG"))
    tx.outputCandidates.zipWithIndex.foreach { case (output, index) =>
      withClue(s"output $index: ") {
        output.value should be >= actualDust(output, index)
      }
    }
  }
}
