package org.ergoplatform.serialization

import io.circe.{DecodingFailure, Json}
import org.ergoplatform.Input
import org.ergoplatform.http.api.ApiCodecs
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.settings.Constants.TrueTree
import org.ergoplatform.utils.ErgoCorePropertyTest
import sigma.VersionContext
import sigma.ast.IntConstant
import sigma.interpreter.ProverResult

class ProverResultJsonDecoderSpec extends ErgoCorePropertyTest with ApiCodecs {

  private val extensionJson = Json.obj("1" -> Json.fromString("040e"))
  private val expectedExtension = Map(1.toByte -> IntConstant(7))
  private val boxId = "0000000000000000000000000000000000000000000000000000000000000000"

  private def withProofBytes(value: Json): Json =
    Json.obj("proofBytes" -> value, "extension" -> extensionJson)

  private def inputJson(proofJson: Json): Json = Json.obj(
    "boxId" -> Json.fromString(boxId),
    "spendingProof" -> proofJson
  )

  private def transactionJson(input: Json): Json = Json.obj(
    "inputs" -> Json.arr(input),
    "dataInputs" -> Json.arr(),
    "outputs" -> Json.arr(Json.obj(
      "value" -> Json.fromLong(1000000L),
      "ergoTree" -> Json.fromString(TrueTree.bytesHex),
      "assets" -> Json.arr(),
      "creationHeight" -> Json.fromInt(1),
      "additionalRegisters" -> Json.obj()
    ))
  )

  private def required[A](where: String, result: Either[DecodingFailure, A]): A =
    result match {
      case Right(value) => value
      case Left(error) => fail(s"$where decoding failed: $error")
    }

  private def checkProof(result: ProverResult, expectedProof: Seq[Byte]): Unit = {
    result.proof.toSeq shouldBe expectedProof
    result.extension.values shouldBe expectedExtension
  }

  private def checkDecoded(proofJson: Json, expectedProof: Seq[Byte]): Unit = {
    VersionContext.withVersions(1.toByte, 1.toByte) {
      val direct = required("ProverResult", proverResultDecoder.decodeJson(proofJson))
      checkProof(direct, expectedProof)

      val input = inputJson(proofJson)
      val decodedInput: Input = required("Input", inputDecoder.decodeJson(input))
      checkProof(decodedInput.spendingProof, expectedProof)

      val tx: ErgoTransaction = required(
        "ErgoTransaction", transactionDecoder.decodeJson(transactionJson(input))
      )
      tx.inputs.size shouldBe 1
      checkProof(tx.inputs.head.spendingProof, expectedProof)
    }
  }

  property("missing proofBytes decodes to empty bytes and keeps the extension") {
    checkDecoded(Json.obj("extension" -> extensionJson), Seq.empty)
  }

  property("null proofBytes decodes to empty bytes and keeps the extension") {
    checkDecoded(withProofBytes(Json.Null), Seq.empty)
  }

  property("empty-string proofBytes decodes to empty bytes and keeps the extension") {
    checkDecoded(withProofBytes(Json.fromString("")), Seq.empty)
  }

  property("nonempty proofBytes retains its exact bytes and the extension") {
    checkDecoded(withProofBytes(Json.fromString("00ff")), Seq(0.toByte, 0xff.toByte))
  }
}
