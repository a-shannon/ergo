package org.ergoplatform.settings

import org.ergoplatform.modifiers.history.extension.ExtensionCandidate
import org.ergoplatform.nodeView.state.{ErgoStateContext, VotingData}
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.generators.ErgoCoreGenerators.defaultHeaderGen
import org.ergoplatform.utils.generators.ErgoNodeTransactionGenerators.validErgoTransactionGenTemplate
import org.ergoplatform.wallet.interpreter.ErgoInterpreter

class ErgoNodeVotingSpecification extends ErgoCorePropertyTest {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.generators.ChainGenerator._

  private val votingEpochLength = 2

  private val hfActivationHeight = 100

  private val hfActivationDifficultyHex = "01"

  implicit val votingSettings: VotingSettings =
    VotingSettings(
      votingEpochLength,
      softForkEpochs = 2,
      activationEpochs = 3,
      version2ActivationHeight = hfActivationHeight,
      version2ActivationDifficultyHex = hfActivationDifficultyHex
    )

  private val updSettings = chainSettings.copy(voting = votingSettings)

  val ctx: ErgoStateContext = {
    new ErgoStateContext(Seq.empty, None, genesisStateDigest, parameters, validationSettingsNoIl, VotingData.empty)(updSettings)
      .upcoming(org.ergoplatform.mining.group.generator, 0L, chainSettings.initialNBits, Array.fill(3)(0.toByte), emptyVSUpdate, 0.toByte)
  }
  val initialVs: ErgoValidationSettings = ctx.validationSettings
  val extensionWithAllParams: ExtensionCandidate = {
    parameters.toExtensionCandidate ++ initialVs.toExtensionCandidate
  }

  val emptyParameters = Parameters(0, Map.empty, ErgoValidationSettingsUpdate.empty)

  property("Parameters should be defined at the beginning of the epoch") {
    val chain = genChain(votingEpochLength * 4).map { b =>
      if (b.header.votingStarts(votingEpochLength)) {
        b.copy(extension = extensionWithAllParams.toExtension(b.header.id))
      } else {
        b
      }
    }
    val validChain = chain.init
    val lastBlock = chain.last
    val invalidExtBlock1 = { // extension does not contain all required params
      lastBlock.copy(extension = lastBlock.extension.copy(
        fields = Seq(Array(0: Byte, 1: Byte) -> Array.fill(4)(2: Byte)))
      )
    }
    val invalidExtBlock2 = { // extension contains redundant parameter
      lastBlock.copy(extension = lastBlock.extension.copy(
        fields = parameters.toExtensionCandidate.fields :+ Array(0: Byte, 99: Byte) -> Array.fill(4)(2: Byte))
      )
    }
    val invalidExtBlock3 = { // extension does not contain params at all
      lastBlock.copy(extension = lastBlock.extension.copy(fields = Seq()))
    }
    val validCtx = validChain.foldLeft(ctx)((acc, mod) => acc.appendFullBlock(mod).get)
    validCtx.appendFullBlock(invalidExtBlock1) shouldBe 'failure
    validCtx.appendFullBlock(invalidExtBlock2) shouldBe 'failure
    validCtx.appendFullBlock(invalidExtBlock3) shouldBe 'failure
    validCtx.appendFullBlock(lastBlock) shouldBe 'success
  }

  property("majority input-cost votes change validation cost of the same transaction") {
    val votes = Array(Parameters.InputCostIncrease, Parameters.NoParameter, Parameters.NoParameter)
    val noVotes = Array.fill(3)(Parameters.NoParameter)
    val h2 = defaultHeaderGen.sample.get.copy(
      height = 2, version = parameters.blockVersion, votes = votes)
    val initialExtension = extensionWithAllParams.toExtension(h2.id)
    val at2 = ctx.process(h2, Some(initialExtension)).get
    val h3 = h2.copy(height = 3, parentId = h2.id)
    val h3WithoutMajority = h3.copy(votes = noVotes)
    val before = at2.appendHeader(h3).get
    val withoutMajority = at2.appendHeader(h3WithoutMajority).get
    val h4 = h3.copy(height = 4, parentId = h3.id, votes = noVotes)
    val h4WithoutMajority = h4.copy(parentId = h3WithoutMajority.id)

    before.votingData.epochVotes.find(_._1 == Parameters.InputCostIncrease).get._2 shouldBe 2
    withoutMajority.votingData.epochVotes.find(_._1 == Parameters.InputCostIncrease).get._2 shouldBe 1
    votingSettings.changeApproved(1) shouldBe false
    votingSettings.changeApproved(2) shouldBe true
    before.currentParameters.height shouldBe 2
    before.currentParameters.inputCost shouldBe 2000

    // The extension is produced from the accumulated votes, then checked again
    // by processExtension against the context's own parameter calculation.
    val upcoming = before.upcoming(h4.minerPk, h4.timestamp, h4.nBits, h4.votes,
      emptyVSUpdate, h4.version)
    val votedCandidate = upcoming.currentParameters.toExtensionCandidate ++
      upcoming.validationSettings.toExtensionCandidate
    val votedExtension = votedCandidate.toExtension(h4.id)
    val unchangedExtension = extensionWithAllParams.toExtension(h4.id)
    val after = before.process(h4, Some(votedExtension)).get
    val control = withoutMajority.process(h4WithoutMajority,
      Some(extensionWithAllParams.toExtension(h4WithoutMajority.id))).get

    after.currentParameters.height shouldBe 4
    after.currentParameters.inputCost shouldBe 2020
    control.currentParameters.inputCost shouldBe before.currentParameters.inputCost
    (after.currentParameters.parametersTable - Parameters.InputCostIncrease) shouldBe
      (before.currentParameters.parametersTable - Parameters.InputCostIncrease)
    after.validationSettings shouldBe before.validationSettings
    before.process(h4, Some(unchangedExtension)) shouldBe 'failure
    withoutMajority.process(h4WithoutMajority,
      Some(votedCandidate.toExtension(h4WithoutMajority.id))) shouldBe 'failure

    val (from, tx) = validErgoTransactionGenTemplate(0, 0, 2).sample.get
    val originalBytes = tx.bytes.clone()
    val originalId = tx.id
    tx.inputs.size shouldBe 2
    tx.statelessValidity().isSuccess shouldBe true
    val beforeResult = tx.statefulValidity(from, emptyDataBoxes, before)(
      ErgoInterpreter(before.currentParameters))
    val afterResult = tx.statefulValidity(from, emptyDataBoxes, after)(
      ErgoInterpreter(after.currentParameters))
    val controlResult = tx.statefulValidity(from, emptyDataBoxes, control)(
      ErgoInterpreter(control.currentParameters))

    beforeResult shouldBe 'success
    afterResult shouldBe 'success
    controlResult shouldBe 'success
    beforeResult.get should be > 0
    afterResult.get should be > 0
    controlResult.get shouldBe beforeResult.get
    (afterResult.get - beforeResult.get) shouldBe (20 * tx.inputs.size)
    tx.id shouldBe originalId
    tx.bytes shouldEqual originalBytes
  }

}
