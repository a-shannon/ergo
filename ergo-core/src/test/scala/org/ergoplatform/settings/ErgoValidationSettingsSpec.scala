package org.ergoplatform.settings

import org.ergoplatform.modifiers.history.extension.Extension
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.ScorexEncoding
import org.ergoplatform.validation.ModifierValidator

import scala.util.{Failure, Success}

class ErgoValidationSettingsSpec extends ErgoCorePropertyTest with ScorexEncoding {

  private val modifierId = scorex.util.bytesToId(Array.fill(32)(0: Byte))

  property("exMatchParameters60 is registered and a settings update disables it") {
    ValidationRules.rulesSpec.get(ValidationRules.exMatchParameters60).map(_.mayBeDisabled) shouldBe Some(true)

    val initial = ErgoValidationSettings.initial
    initial.isActive(ValidationRules.exMatchParameters60) shouldBe true

    val update = ErgoValidationSettingsUpdate(
      Seq(ValidationRules.exMatchParameters60),
      Seq.empty
    )
    val updated = initial.updated(update)
    updated.isActive(ValidationRules.exMatchParameters60) shouldBe false

    val parsed = ErgoValidationSettings.parseExtension(updated.toExtensionCandidate).get
    parsed.isActive(ValidationRules.exMatchParameters60) shouldBe false
    parsed.updateFromInitial.rulesToDisable should contain(
      ValidationRules.exMatchParameters60
    )
  }

  property("exMatchParameters60 validateNoFailure reports the rule message") {
    val valid = ModifierValidator(ErgoValidationSettings.initial)
      .validateNoFailure(
        ValidationRules.exMatchParameters60,
        Success(()),
        modifierId,
        Extension.modifierTypeId
      )
      .result
    valid.isValid shouldBe true

    val invalid = ModifierValidator(ErgoValidationSettings.initial)
      .validateNoFailure(
        ValidationRules.exMatchParameters60,
        Failure(new IllegalArgumentException("missing parameter 130")),
        modifierId,
        Extension.modifierTypeId
      )
      .result

    invalid.isValid shouldBe false
    invalid.errors.head.isFatal shouldBe true
    invalid.errors.head.message should include("the extension should contain all the system parameters, possibly more")
    invalid.errors.head.message should include("missing parameter 130")
  }

}
