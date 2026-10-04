package org.ergoplatform.mining

import org.ergoplatform.{ErgoBoxCandidate, ErgoBox}
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.settings.{Constants, MainnetLaunchParameters, Parameters}
import org.ergoplatform.utils.ErgoCoreTestConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.bytesToId
import sigma.Colls

/** Checks the actual serialized output size used by the consensus dust rule. */
class StorageRentClaimDustBoundarySpec extends AnyFlatSpec with Matchers {

  private val parameters = MainnetLaunchParameters
  private val claimHeight = 3 * Constants.StoragePeriod
  private val dummyTxId = bytesToId(Array.fill(32)(0.toByte))

  private def claimFor(inputValue: Long): Option[ErgoTransaction] = {
    val box: ErgoBox = new ErgoBoxCandidate(inputValue, Constants.TrueTree, 0,
      Colls.emptyColl, Map.empty).toBox(dummyTxId, 0.toShort)
    StorageRentClaimBuilder.buildClaim(Seq(box), claimHeight, parameters,
      ErgoCoreTestConstants.defaultMinerPk, None, Set.empty)
  }

  private def actualDust(tx: ErgoTransaction, outputIndex: Int,
                         currentParameters: Parameters = parameters): Long =
    tx.outputCandidates(outputIndex).toBox(dummyTxId, outputIndex.toShort).bytes.length.toLong *
      currentParameters.minValuePerByte

  "A storage-rent claim" should "skip a claim whose proceeds cannot clear actual dust" in {
    claimFor(45000L) shouldBe None
  }

  it should "keep a higher-value control claim above actual dust" in {
    val tx = claimFor(46000L).getOrElse(fail("expected a claim for 46000 nanoERG"))
    tx.outputCandidates.zipWithIndex.foreach { case (output, index) =>
      withClue(s"output $index: ") {
        output.value should be >= actualDust(tx, index)
      }
    }
  }

  it should "remeasure a recreated box after its value crosses a VLQ boundary" in {
    val changedParameters = new Parameters(parameters.height,
      parameters.parametersTable.updated(Parameters.MinValuePerByteIncrease, 350),
      parameters.proposedUpdate)
    val boxes = Seq(20000L, 10000000000L).zipWithIndex.map { case (value, index) =>
      new ErgoBoxCandidate(value, Constants.TrueTree, 0,
        Colls.emptyColl, Map.empty).toBox(dummyTxId, index.toShort)
    }
    val tx = StorageRentClaimBuilder.buildClaim(boxes, claimHeight, changedParameters,
      ErgoCoreTestConstants.defaultMinerPk, None, Set.empty)
      .getOrElse(fail("expected a claim funded by the second box"))
    withClue(s"recreated value ${tx.outputCandidates.head.value}: ") {
      tx.outputCandidates.head.value shouldBe actualDust(tx, 0, changedParameters)
    }
  }
}
