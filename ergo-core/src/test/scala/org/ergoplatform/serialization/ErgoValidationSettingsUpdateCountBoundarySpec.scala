package org.ergoplatform.serialization

import org.ergoplatform.settings.ErgoValidationSettingsUpdate
import org.ergoplatform.settings.ErgoValidationSettingsUpdateSerializer
import org.ergoplatform.utils.ErgoCorePropertyTest
import scorex.util.ByteArrayBuilder
import scorex.util.serialization.VLQByteBufferWriter

class ErgoValidationSettingsUpdateCountBoundarySpec extends ErgoCorePropertyTest {

  private val countAboveIntMax = 1L << 31

  private def encodedCounts(disabled: Long, statuses: Long): Array[Byte] = {
    val writer = new VLQByteBufferWriter(new ByteArrayBuilder())
    writer.putUInt(disabled)
    writer.putUInt(statuses)
    writer.result().toBytes
  }

  property("canonical empty update keeps its two zero counts") {
    val bytes = encodedCounts(0L, 0L)
    val parsed = ErgoValidationSettingsUpdateSerializer.parseBytesTry(bytes).get

    parsed shouldBe ErgoValidationSettingsUpdate.empty
    val canonical = ErgoValidationSettingsUpdateSerializer.toBytes(parsed)
    canonical.sameElements(bytes) shouldBe true
  }

  property("oversized disabled-rules count currently narrows to an empty update") {
    val bytes = encodedCounts(countAboveIntMax, 0L)
    val parsed = ErgoValidationSettingsUpdateSerializer.parseBytesTry(bytes).get

    bytes.length shouldBe 6
    parsed shouldBe ErgoValidationSettingsUpdate.empty
    val canonical = ErgoValidationSettingsUpdateSerializer.toBytes(parsed)
    canonical.sameElements(bytes) shouldBe false
  }

  property("oversized status-updates count currently narrows to an empty update") {
    val bytes = encodedCounts(0L, countAboveIntMax)
    val parsed = ErgoValidationSettingsUpdateSerializer.parseBytesTry(bytes).get

    bytes.length shouldBe 6
    parsed shouldBe ErgoValidationSettingsUpdate.empty
    val canonical = ErgoValidationSettingsUpdateSerializer.toBytes(parsed)
    canonical.sameElements(bytes) shouldBe false
  }
}
