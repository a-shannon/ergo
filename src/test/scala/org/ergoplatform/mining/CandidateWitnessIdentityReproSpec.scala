package org.ergoplatform.mining

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.ergoplatform.{ErgoBoxCandidate, Input, UnsignedInput}
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction, UnsignedErgoTransaction}
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.mempool.ErgoMemPoolUtils.ProcessingOutcome
import org.ergoplatform.nodeView.state.BoxHolder
import org.ergoplatform.settings.Constants.TrueTree
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.ErgoCoreTestConstants._
import org.ergoplatform.utils.ErgoNodeTestConstants.settings
import org.ergoplatform.utils.generators.ValidBlocksGenerators.createUtxoState
import scorex.util.{ModifierId, bytesToId}
import sigma.ast.ErgoTree
import sigma.interpreter.ProverResult

class CandidateWitnessIdentityReproSpec extends ErgoCorePropertyTest {
  property("same-ID signature variants across candidate collection and mempool invalidation") {
    val funding = new ErgoBoxCandidate(1000000000L,
      ErgoTree.fromSigmaBoolean(defaultMinerPk), 0)
      .toBox(bytesToId(Array.fill(32)(1.toByte)), 0.toShort)
    val state = createUtxoState(BoxHolder(Seq(funding)), parameters)
    try {
      val context = state.stateContext.upcoming(defaultMinerPk.value, defaultTimestamp,
        defaultNBits, defaultVotes, emptyVSUpdate, 4.toByte)
      val unsigned = UnsignedErgoTransaction(IndexedSeq(new UnsignedInput(funding.id)),
        IndexedSeq(new ErgoBoxCandidate(999000000L, TrueTree, 0),
          new ErgoBoxCandidate(1000000L, feeProp, 0)))
      val valid = ErgoTransaction(defaultProver.sign(unsigned, IndexedSeq(funding),
        IndexedSeq.empty, context).get)
      val invalid = valid.copy(inputs = valid.inputs.map { input =>
        Input(input.boxId, ProverResult(Array.emptyByteArray, input.spendingProof.extension))
      })
      val child = ErgoTransaction(IndexedSeq(Input(valid.outputs.head.id, ProverResult.empty)),
        IndexedSeq(new ErgoBoxCandidate(998000000L, TrueTree, 0),
          new ErgoBoxCandidate(1000000L, feeProp, 0)))

      valid.id shouldBe invalid.id
      valid.messageToSign.toSeq shouldBe invalid.messageToSign.toSeq
      valid.inputs.head.spendingProof.proof.nonEmpty shouldBe true
      invalid.inputs.head.spendingProof.proof shouldBe empty
      valid.witnessSerializedId.toSeq should not be invalid.witnessSerializedId.toSeq
      valid.bytes.toSeq should not be invalid.bytes.toSeq
      valid.statelessValidity().isSuccess shouldBe true
      invalid.statelessValidity().isSuccess shouldBe true
      state.validateWithCost(valid, context, Int.MaxValue, None).isSuccess shouldBe true
      val invalidResult = state.validateWithCost(invalid, context, Int.MaxValue, None)
      invalidResult.isFailure shouldBe true
      invalidResult.failed.get.getMessage.toLowerCase should include("script")
      state.withTransactions(Seq(valid)).validateWithCost(child, context,
        Int.MaxValue, None).isSuccess shouldBe true

      val (pool, outcome) = ErgoMemPool.empty(settings).process(UnconfirmedTransaction(valid, None), state)
      outcome.isInstanceOf[ProcessingOutcome.Accepted] shouldBe true
      pool.contains(valid.id) shouldBe true

      type Collected = (Seq[ErgoTransaction], Seq[ModifierId])
      def upstream(txs: Seq[ErgoTransaction]): Collected =
        CandidateGenerator.collectTxs(defaultMinerPk, Int.MaxValue, Int.MaxValue, state, context, txs)
      def copied(txs: Seq[ErgoTransaction]): Collected =
        CandidateCollectorBaselineCopy.collectTxs(defaultMinerPk, Int.MaxValue, Int.MaxValue, state, context, txs)
      def acceptedOnly(txs: Seq[ErgoTransaction]): Collected =
        CandidateCollectorAcceptedOnly.collectTxs(defaultMinerPk, Int.MaxValue, Int.MaxValue, state, context, txs)
      def firstSeen(txs: Seq[ErgoTransaction]): Collected = {
        val seen = scala.collection.mutable.HashSet.empty[ModifierId]
        upstream(txs.filter(tx => seen.add(tx.id)))
      }

      case class Case(name: String, input: Seq[ErgoTransaction], selected: Boolean,
                      childSelected: Boolean, eliminated: Boolean)
      val baselineCases = Seq(
        Case("valid", Seq(valid), true, false, false),
        Case("valid-child", Seq(valid, child), true, true, false),
        Case("valid-child-valid", Seq(valid, child, valid), true, true, true),
        Case("invalid-valid", Seq(invalid, valid), true, false, true),
        Case("valid-invalid", Seq(valid, invalid), true, false, true),
        Case("invalid-only", Seq(invalid), false, false, true))
      val modes: Seq[(String, Seq[ErgoTransaction] => Collected, Seq[Case])] = Seq(
        ("upstream", upstream _, baselineCases),
        ("first-seen", firstSeen _, baselineCases.map {
          case c if c.name == "valid-child-valid" || c.name == "valid-invalid" => c.copy(eliminated = false)
          case c if c.name == "invalid-valid" => c.copy(selected = false)
          case c => c
        }),
        ("accepted-only", acceptedOnly _, baselineCases.map {
          case c if c.name == "valid-child-valid" || c.name == "valid-invalid" => c.copy(eliminated = false)
          case c => c
        }))

      val rows = scala.collection.mutable.ArrayBuffer(
        "mode\tinput\tvalidSelected\tchildSelected\tparentEliminated\tparentInPool\tfundingUnspent")
      modes.foreach { case (mode, collect, cases) =>
        cases.foreach { c =>
          withClue(s"$mode / ${c.name}: ") {
            val result = collect(c.input)
            if (mode == "upstream") {
              val control = copied(c.input)
              control._1.map(_.bytes.toSeq) shouldBe result._1.map(_.bytes.toSeq)
              control._2 shouldBe result._2
            }
            val (selected, eliminated) = result
            val included = selected.filter(_.id == valid.id)
            included.size shouldBe (if (c.selected) 1 else 0)
            included.foreach(_.bytes.toSeq shouldBe valid.bytes.toSeq)
            selected.exists(_.id == child.id) shouldBe c.childSelected
            eliminated.contains(valid.id) shouldBe c.eliminated
            eliminated.filterNot(_ == valid.id) shouldBe empty
            val after = eliminated.foldLeft(pool)((p, id) => p.invalidate(id))
            after.contains(valid.id) shouldBe !c.eliminated
            state.boxById(funding.id).isDefined shouldBe true
            val row = s"$mode\t${c.name}\t${included.nonEmpty}\t${c.childSelected}\t${c.eliminated}\t${after.contains(valid.id)}\ttrue"
            rows += row
            info(row)
          }
        }
      }
      val output = Paths.get(sys.props("repro.output"))
      Files.createDirectories(output)
      Files.write(output.resolve("matrix.tsv"), rows.mkString("\n").getBytes(StandardCharsets.UTF_8))
      Files.write(output.resolve("valid-parent.bin"), valid.bytes)
      Files.write(output.resolve("invalid-parent.bin"), invalid.bytes)
      Files.write(output.resolve("child.bin"), child.bytes)
      Files.write(output.resolve("funding.bin"), funding.bytes)
      Files.write(output.resolve("context.txt"),
        s"height=${context.currentHeight}\nversion=${context.blockVersion}\ntimestamp=$defaultTimestamp\nnBits=$defaultNBits\nparameters=MainnetLaunchParameters\n".getBytes(StandardCharsets.UTF_8))
    } finally {
      state.closeStorage()
    }
  }
}
