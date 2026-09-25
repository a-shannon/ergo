package org.ergoplatform.it.llm_generated

import io.circe.Json
import io.circe.parser.parse
import org.ergoplatform.it.PrunedDigestInfo
import org.ergoplatform.it.api.NodeApi
import org.scalatest.OptionValues
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrunedDigestInfoSpec extends AnyFlatSpec with Matchers with OptionValues {
  private val body =
    """{"bestHeaderId":"tip-b","bestFullHeaderId":"tip-b","headersHeight":10,
      |"fullHeight":10,"stateRoot":"root-a","stateVersion":"tip-a","isMining":false}""".stripMargin

  private def json: Json = parse(body).fold(throw _, identity)

  "Digest info diagnostics" should "retain the version from the same response as the compared fields" in {
    val snapshot = PrunedDigestInfo.decodeSnapshot(body)
    snapshot.info shouldBe NodeApi.nodeInfoDecoder.decodeJson(json).fold(throw _, identity)
    snapshot.info.bestBlockIdOpt shouldBe Some("tip-b")
    snapshot.info.stateRootOpt shouldBe Some("root-a")
    snapshot.stateVersion shouldBe Some(Json.fromString("tip-a"))
  }

  it should "tolerate absent and null diagnostic versions" in {
    val absent = json.mapObject(_.remove("stateVersion"))
    val nulled = json.mapObject(_.add("stateVersion", Json.Null))
    PrunedDigestInfo.decodeSnapshot(absent.noSpaces).stateVersion shouldBe None
    PrunedDigestInfo.decodeSnapshot(nulled.noSpaces).stateVersion shouldBe Some(Json.Null)
  }

  it should "keep the original equality independent of the added diagnostic field" in {
    val sample = PrunedDigestInfo.decodeSnapshot(body)
    val digest = PrunedDigestInfo.decodeSnapshot(
      json.mapObject(_.add("stateVersion", Json.fromString("tip-b"))).noSpaces)
    digest.stateVersion should not be sample.stateVersion
    withClue(PrunedDigestInfo.comparisonClue(sample, digest)) {
      digest.info shouldEqual sample.info
    }
  }

  it should "preserve a root mismatch and report both original versions" in {
    val digest = PrunedDigestInfo.decodeSnapshot(body)
    val sample = PrunedDigestInfo.decodeSnapshot(json.mapObject(_
      .add("stateRoot", Json.fromString("root-b"))
      .add("stateVersion", Json.fromString("tip-b"))).noSpaces)
    val error = intercept[TestFailedException] {
      withClue(PrunedDigestInfo.comparisonClue(sample, digest)) {
        digest.info shouldEqual sample.info
      }
    }
    error.message.value should include("source stateVersion=\"tip-b\"")
    error.message.value should include("digest stateVersion=\"tip-a\"")
    error.message.value should include("did not equal")
    error.failedCodeFileName.value shouldBe "PrunedDigestInfoSpec.scala"
  }

  it should "escape control characters instead of injecting extra diagnostic lines" in {
    val snapshot = PrunedDigestInfo.decodeSnapshot(
      json.mapObject(_.add("stateVersion", Json.fromString("tip\nforged"))).noSpaces)
    PrunedDigestInfo.comparisonClue(snapshot, snapshot) should not include "\n"
    PrunedDigestInfo.comparisonClue(snapshot, snapshot) should include("tip\\nforged")
  }

  it should "preserve decoder rejection of malformed original fields" in {
    val malformed = json.mapObject(_.add("fullHeight", Json.fromString("wrong")))
    NodeApi.nodeInfoDecoder.decodeJson(malformed).isLeft shouldBe true
    intercept[io.circe.Error](PrunedDigestInfo.decodeSnapshot(malformed.noSpaces))
  }

  it should "keep malformed diagnostic fields from masking the original root mismatch" in {
    val sample = PrunedDigestInfo.decodeSnapshot(body)
    Seq(Json.fromInt(42), Json.obj("unexpected" -> Json.True)).foreach { version =>
      val malformed = json.mapObject(_
        .add("stateVersion", version)
        .add("stateRoot", Json.fromString("root-b")))
      val digest = PrunedDigestInfo.decodeSnapshot(malformed.noSpaces)
      digest.info shouldBe NodeApi.nodeInfoDecoder.decodeJson(malformed).fold(throw _, identity)
      val error = intercept[TestFailedException] {
        withClue(PrunedDigestInfo.comparisonClue(sample, digest)) {
          digest.info shouldEqual sample.info
        }
      }
      error.message.value should include("did not equal")
      error.message.value should include(s"digest stateVersion=${version.noSpaces}")
    }
  }
}
