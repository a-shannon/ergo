package org.ergoplatform.mining.llm_generated

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.StatusReply
import akka.testkit.{TestKit, TestProbe}
import org.ergoplatform.{AutolykosSolution, InputSolutionFound, OrderingSolutionFound, SolutionFound}
import org.ergoplatform.mining.{CandidateGenerator, DefaultFakePowScheme}
import org.ergoplatform.mining.CandidateGenerator.{Candidate, GenerateCandidate}
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.NewBlockMined
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef, LocallyGeneratedInputBlock, LocallyGeneratedOrderingBlock}
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.GetDataFromCurrentView
import org.ergoplatform.nodeView.state.StateType
import org.ergoplatform.settings.{ErgoSettings, ErgoSettingsReader, Parameters}
import org.ergoplatform.utils.ErgoCoreTestConstants.defaultMinerSecret
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.ModifierId
import sigma.data.ProveDlog
import sigmastate.crypto.DLogProtocol.DLogProverInput

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Actor submission guards, using controlled validation predicates, not real PoW evidence. */
class MatrixSolutionValidationSpec extends AnyFlatSpec with Matchers {
  private val baseSettings: ErgoSettings = {
    val settings = ErgoSettingsReader.read()
    settings.copy(
      nodeSettings = settings.nodeSettings.copy(
        mining = true,
        stateType = StateType.Utxo,
        offlineGeneration = true,
        verifyTransactions = true
      ),
      chainSettings = settings.chainSettings.copy(blockInterval = 1.second)
    )
  }

  private class ControlledPow extends DefaultFakePowScheme(
    baseSettings.chainSettings.powScheme.k,
    baseSettings.chainSettings.powScheme.n
  ) {
    private val rejected = TrieMap.empty[ModifierId, Unit]
    val orderingChecks = new ConcurrentLinkedQueue[ModifierId]()
    val inputChecks = new AtomicInteger(0)
    @volatile var acceptInputs = true

    def reject(header: Header): Unit = { rejected.put(header.id, ()); () }
    def accept(header: Header): Unit = { rejected.remove(header.id); () }

    override def validate(header: Header): Try[Unit] = {
      orderingChecks.add(header.id)
      if (rejected.contains(header.id)) Failure(new Exception("Controlled PoW rejection"))
      else Success(())
    }

    override def checkInputBlockPoW(header: Header, parameters: Parameters): Boolean = {
      inputChecks.incrementAndGet()
      acceptInputs
    }
  }

  private class Harness extends TestKit(ActorSystem()) {
    val replies = TestProbe()
    val view = TestProbe()
    val announcements = TestProbe()
    system.eventStream.subscribe(announcements.ref, classOf[NewBlockMined])
    val pow = new ControlledPow
    val settings = baseSettings.copy(
      directory = s"${baseSettings.directory}-matrix-validation-${java.util.UUID.randomUUID()}",
      chainSettings = baseSettings.chainSettings.copy(powScheme = pow)
    )
    // Candidate construction uses actual state, history, and mempool readers.
    val realView: ActorRef = ErgoNodeViewRef(settings)
    val readers: ActorRef = ErgoReadersHolderRef(realView)
    val generator: ActorRef = CandidateGenerator(
      defaultMinerSecret.publicImage, readers, view.ref, settings
    )
    val solution = new AutolykosSolution(
      defaultMinerSecret.publicImage.value,
      defaultMinerSecret.publicImage.value,
      Array.fill[Byte](8)(0),
      BigInt(0)
    )

    def solutionWithNonce(nonce: Array[Byte]): AutolykosSolution =
      new AutolykosSolution(solution.pk, solution.w, nonce, solution.d)

    def candidate(forced: Boolean = false, pk: Option[ProveDlog] = None): Candidate = {
      generator.tell(GenerateCandidate(Seq.empty, reply = true, forced = forced, optPk = pk), replies.ref)
      replies.expectMsgPF(10.seconds) {
        case StatusReply.Success(c: Candidate) => c
      }
    }

    def submit(message: SolutionFound): Unit = generator.tell(message, replies.ref)

    def errorContaining(text: String): Unit = {
      replies.expectMsgPF(3.seconds) {
        case StatusReply.Error(error) => error.getMessage should include(text)
      }
    }

    def noEffects(): Unit = {
      view.expectNoMessage(100.millis)
      announcements.expectNoMessage(100.millis)
    }

    def assertOrderingAccepted(candidate: Candidate): Unit = {
      val block = CandidateGenerator.completeOrderingBlock(candidate.candidateBlock, solution)
      submit(OrderingSolutionFound(solution))
      replies.expectMsg(3.seconds, StatusReply.success(()))
      val emitted = view.expectMsgType[LocallyGeneratedOrderingBlock](3.seconds)
      emitted.efb.id shouldBe block.id
      emitted.orderingBlockTransactions.map(_.id) shouldBe
        candidate.candidateBlock.orderingBlockTransactions.map(_.id)
      announcements.expectMsg(NewBlockMined(block.header))
    }

    def assertTypedErrors(reason: String): Unit = {
      Seq(InputSolutionFound(solution), OrderingSolutionFound(solution)).foreach { message =>
        submit(message)
        errorContaining(reason)
      }
      noEffects()
    }
  }

  private def withHarness(run: Harness => Unit): Unit = {
    val harness = new Harness
    try run(harness)
    finally TestKit.shutdownActorSystem(harness.system)
  }

  Seq("input", "ordering").foreach { kind =>
    it should s"reject $kind nonces of length 0, 1 and 9 without restart, cache loss or emissions" in withHarness { h =>
      val cached = h.candidate()
      h.pow.orderingChecks.clear()
      h.pow.inputChecks.set(0)

      Seq(0, 1, 9).foreach { length =>
        val malformed = h.solutionWithNonce(Array.fill[Byte](length)(1))
        h.submit(if (kind == "input") InputSolutionFound(malformed) else OrderingSolutionFound(malformed))
        h.errorContaining("Invalid solution nonce length: expected 8 bytes")
        h.noEffects()
        h.candidate() should be theSameInstanceAs cached
      }
      h.pow.orderingChecks.asScala.toSeq shouldBe empty
      h.pow.inputChecks.get() shouldBe 0

      if (kind == "input") {
        h.submit(InputSolutionFound(h.solution))
        h.replies.expectMsg(3.seconds, StatusReply.success(()))
        val emitted = h.view.expectMsgType[LocallyGeneratedInputBlock](3.seconds)
        emitted.sbi.header.id shouldBe CandidateGenerator.completeInputBlock(cached.candidateBlock, h.solution)._1.header.id
        emitted.sbt.transactions.map(_.id) shouldBe cached.candidateBlock.inputBlockTransactions.map(_.id)
        h.pow.inputChecks.get() shouldBe 1
        h.view.expectMsgType[GetDataFromCurrentView[_, _]]
        h.submit(InputSolutionFound(h.solution))
        h.errorContaining("Input block pending application")
        h.noEffects()
        h.assertOrderingAccepted(cached)
        h.assertTypedErrors("Block already solved")
      } else {
        val block = CandidateGenerator.completeOrderingBlock(cached.candidateBlock, h.solution)
        h.submit(OrderingSolutionFound(h.solution))
        h.replies.expectMsg(3.seconds, StatusReply.success(()))
        h.view.expectMsg(LocallyGeneratedOrderingBlock(block, cached.candidateBlock.orderingBlockTransactions))
        h.announcements.expectMsg(NewBlockMined(block.header))
        h.assertTypedErrors("Block already solved")
      }
    }
  }

  it should "retain the first candidate after invalid ordering PoW and accept a later valid solution" in withHarness { h =>
    val cached = h.candidate()
    val block = CandidateGenerator.completeOrderingBlock(cached.candidateBlock, h.solution)
    h.pow.reject(block.header)
    h.pow.orderingChecks.clear()

    h.submit(OrderingSolutionFound(h.solution))
    h.errorContaining("No retained candidate matches ordering solution PoW")
    h.noEffects()
    h.pow.orderingChecks.asScala.toSeq shouldBe Seq(block.header.id)
    h.candidate() should be theSameInstanceAs cached

    h.pow.accept(block.header)
    h.submit(OrderingSolutionFound(h.solution))
    h.replies.expectMsg(3.seconds, StatusReply.success(()))
    h.view.expectMsg(LocallyGeneratedOrderingBlock(block, cached.candidateBlock.orderingBlockTransactions))
    h.announcements.expectMsg(NewBlockMined(block.header))
    h.assertTypedErrors("Block already solved")
  }

  Seq(false, true).foreach { rejectPrevious =>
    it should s"validate current then previous ordering PoW and preserve source transactions (rejectPrevious=$rejectPrevious)" in withHarness { h =>
      val previous = h.candidate()
      val previousBlock = CandidateGenerator.completeOrderingBlock(previous.candidateBlock, h.solution)
      val otherPk = DLogProverInput(BigInt(17).bigInteger).publicImage
      val current = h.candidate(forced = true, pk = Some(otherPk))
      val currentBlock = CandidateGenerator.completeOrderingBlock(current.candidateBlock, h.solution)
      currentBlock.header.id should not be previousBlock.header.id
      current.candidateBlock.orderingBlockTransactions.map(_.id) should not be (
        previous.candidateBlock.orderingBlockTransactions.map(_.id)
      )

      // Both caches must survive malformed submissions, including the fallback source.
      Seq(InputSolutionFound(h.solutionWithNonce(Array.emptyByteArray)),
        OrderingSolutionFound(h.solutionWithNonce(Array.fill[Byte](9)(1)))).foreach { malformed =>
        h.submit(malformed)
        h.errorContaining("nonce")
      }
      h.candidate(pk = Some(otherPk)) should be theSameInstanceAs current
      h.noEffects()

      h.pow.reject(currentBlock.header)
      if (rejectPrevious) h.pow.reject(previousBlock.header)
      h.pow.orderingChecks.clear()
      h.submit(OrderingSolutionFound(h.solution))

      if (rejectPrevious) {
        h.errorContaining("No retained candidate matches ordering solution PoW")
        h.pow.orderingChecks.asScala.toSeq shouldBe Seq(currentBlock.header.id, previousBlock.header.id)
        h.pow.acceptInputs = false
        h.submit(InputSolutionFound(h.solution))
        h.errorContaining("No retained candidate matches input solution PoW")
        h.submit(OrderingSolutionFound(h.solution))
        h.errorContaining("No retained candidate matches ordering solution PoW")
        h.noEffects()
        // Invalid submissions preserve the current work and the retained fallback.
        val regenerated = h.candidate(pk = Some(otherPk))
        (regenerated eq current) shouldBe true
        (regenerated eq previous) shouldBe false
      } else {
        h.replies.expectMsg(3.seconds, StatusReply.success(()))
        h.pow.orderingChecks.asScala.toSeq shouldBe Seq(currentBlock.header.id, previousBlock.header.id)
        val emitted = h.view.expectMsgType[LocallyGeneratedOrderingBlock](3.seconds)
        emitted.efb.id shouldBe previousBlock.id
        emitted.orderingBlockTransactions.map(_.id) shouldBe previous.candidateBlock.orderingBlockTransactions.map(_.id)
        h.announcements.expectMsg(NewBlockMined(previousBlock.header))
        h.assertTypedErrors("Block already solved")
      }
    }
  }

  it should "reject invalid input PoW without emissions and accept ordering work from the retained cache" in withHarness { h =>
    val cached = h.candidate()
    h.pow.acceptInputs = false
    h.pow.inputChecks.set(0)
    h.submit(InputSolutionFound(h.solution))
    h.errorContaining("No retained candidate matches input solution PoW")
    h.pow.inputChecks.get() shouldBe 1
    h.noEffects()
    h.submit(InputSolutionFound(h.solution))
    h.errorContaining("No retained candidate matches input solution PoW")
    h.noEffects()
    (h.candidate() eq cached) shouldBe true
    h.assertOrderingAccepted(cached)
    h.assertTypedErrors("Block already solved")
  }
}
