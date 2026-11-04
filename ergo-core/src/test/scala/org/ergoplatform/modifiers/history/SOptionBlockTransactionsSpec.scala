package org.ergoplatform.modifiers.history

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.validation.ValidationRules.CheckV6Type
import sigma.VersionContext
import sigma.ast.{BoolToSigmaProp, Constant, EQ, ErgoTree, SInt, SOption}
import sigma.data.TrivialProp.TrueProp
import sigma.interpreter.ProverResult
import sigma.serialization.SerializerException
import sigma.validation.ValidationException

class SOptionBlockTransactionsSpec extends ErgoCorePropertyTest {

  private val amount = 10000000L
  private val height = 1
  private val optionType = SOption(SInt)

  private def optionScript: ErgoTree = {
    val option = Constant[SOption[SInt.type]](Some(1), optionType)
    val proposition = BoolToSigmaProp(EQ(option, option))
    ErgoTree.fromProposition(ErgoTree.defaultHeaderWithVersion(3.toByte), proposition)
  }

  private def transaction(
    script: ErgoTree,
    registers: ErgoBox.AdditionalRegisters = Map.empty
  ): ErgoTransaction = {
    val source = new ErgoBoxCandidate(amount, ErgoTree.fromSigmaBoolean(TrueProp), height)
      .toBox(Header.GenesisParentId, 0.toShort)
    val output = new ErgoBoxCandidate(
      amount, script, height, additionalRegisters = registers
    )
    ErgoTransaction(
      IndexedSeq(Input(source.id, ProverResult.empty)),
      IndexedSeq.empty,
      IndexedSeq(output)
    )
  }

  property("a version four section parses an SOption script constant") {
    val (currentBytes, earlierBytes) = VersionContext.withVersions(3.toByte, 3.toByte) {
      val tx = transaction(optionScript)
      val current = BlockTransactions(Header.GenesisParentId, 4.toByte, Seq(tx))
      val earlier = BlockTransactions(Header.GenesisParentId, 3.toByte, Seq(tx))
      (current.bytes, earlier.bytes)
    }

    val parsedCurrent = BlockTransactionsSerializer.parseBytesTry(currentBytes).get
    val parsedTree = parsedCurrent.txs.head.outputCandidates.head.ergoTree

    parsedTree.isRightParsed shouldBe true
    parsedTree.version shouldBe 3.toByte
    val optionFound = parsedTree.constants.exists { c =>
      c.tpe == optionType && c.value == Some(1)
    }
    optionFound shouldBe true
    parsedCurrent.bytes shouldBe currentBytes

    val plainEarlierBytes = VersionContext.withVersions(3.toByte, 3.toByte) {
      val plainTree = ErgoTree.fromSigmaBoolean(
        ErgoTree.defaultHeaderWithVersion(3.toByte), TrueProp
      )
      val plainTx = transaction(plainTree)
      BlockTransactions(Header.GenesisParentId, 3.toByte, Seq(plainTx)).bytes
    }

    val versionGateMessage = "Tree version (3) is above activated script version (2)"
    Seq(earlierBytes, plainEarlierBytes).foreach { bytes =>
      val error = VersionContext.withVersions(2.toByte, 1.toByte) {
        BlockTransactionsSerializer.parseBytesTry(bytes).failed.get
      }
      error match {
        case e: SerializerException => e.message shouldBe versionGateMessage
        case other => fail(s"Expected tree-version rejection, got $other")
      }
    }
  }

  property("a version four section still rejects an SOption register") {
    val sectionBytes = VersionContext.withVersions(3.toByte, 3.toByte) {
      val option = Constant[SOption[SInt.type]](Some(1), optionType)
      val registers: ErgoBox.AdditionalRegisters = Map(ErgoBox.R4 -> option)
      val plainScript = ErgoTree.fromSigmaBoolean(
        ErgoTree.defaultHeaderWithVersion(3.toByte), TrueProp
      )
      val tx = transaction(plainScript, registers)
      BlockTransactions(Header.GenesisParentId, 4.toByte, Seq(tx)).bytes
    }

    BlockTransactionsSerializer.parseBytesTry(sectionBytes).failed.get match {
      case e: ValidationException => e.rule.id shouldBe CheckV6Type.id
      case other => fail(s"Expected CheckV6Type rejection, got $other")
    }
  }
}
