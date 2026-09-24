package org.ergoplatform.mining.llm_generated

import org.ergoplatform.mining.{AutolykosPowScheme, CandidateBlock, CandidateGenerator, ErgoMiningThread, PrivateKey}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.StatusReply
import akka.testkit.{TestKit, TestProbe}
import com.google.common.primitives.Longs
import org.ergoplatform.{AutolykosSolution, InputBlockFound, InputSolutionFound, OrderingSolutionFound, ProveBlockResult}
import org.ergoplatform.mining.CandidateGenerator.{Candidate, GenerateCandidate}
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.{ChangedHistory, ChangedState, LocalBlockApplied, NewBestInputBlock}
import org.ergoplatform.nodeView.{LocallyGeneratedInputBlock, LocallyGeneratedOrderingBlock}
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{ErgoStateContext, StateType, UtxoState}
import org.ergoplatform.nodeView.wallet.ErgoWalletReader
import org.ergoplatform.settings.Parameters
import org.ergoplatform.utils.{HistoryTestHelpers, RandomWrapper}
import org.ergoplatform.utils.generators.ChainGenerator.applyChain
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock, validTransactionsFromBoxHolder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.Try

/** Real actor, candidate assembly and stores; deterministic PoW controls the race boundary.
  * No network actors are started. Input application uses the same history API as the holder.
  */
class CandidateRetainedWorkSpec extends AnyFlatSpec with Matchers {
  import org.ergoplatform.utils.ErgoCoreTestConstants.defaultMinerSecret
  import org.ergoplatform.utils.ErgoNodeTestConstants.settings

  behavior of "CandidateGenerator"

  private class ControlledPow extends AutolykosPowScheme(32, 26) {
    @volatile var expectedParameters: Map[Long, Int] = Map.empty
    @volatile var acceptEveryNonce: Boolean = false
    @volatile var checked: Vector[(Long, Int)] = Vector.empty

    override def checkInputBlockPoW(header: Header, parameters: Parameters): Boolean = {
      checked :+= header.timestamp -> parameters.subBlocksPerBlock
      acceptEveryNonce || (Longs.fromByteArray(header.powSolution.n) == header.timestamp &&
        expectedParameters.get(header.timestamp).contains(parameters.subBlocksPerBlock))
    }

    override def validate(header: Header): Try[Unit] = Try {
      require(Longs.fromByteArray(header.powSolution.n) == header.timestamp, "Wrong work")
    }

    override def proveCandidate(block: CandidateBlock, sk: PrivateKey,
                                minNonce: Long, maxNonce: Long,
                                parameters: Parameters): ProveBlockResult = {
      expectedParameters += block.timestamp -> parameters.subBlocksPerBlock
      InputBlockFound(CandidateGenerator.completeOrderingBlock(block, solution(block.timestamp)))
    }
  }

  private def solution(nonce: Long): AutolykosSolution = new AutolykosSolution(
    defaultMinerSecret.publicImage.value, defaultMinerSecret.publicImage.value,
    Longs.toByteArray(nonce), BigInt(0)
  )

  private class Fixture(cacheSize: Int = 3) extends TestKit(ActorSystem()) {
    val pow = new ControlledPow
    val config = settings.copy(
      chainSettings = settings.chainSettings.copy(powScheme = pow),
      nodeSettings = settings.nodeSettings.copy(offlineGeneration = true,
        internalMinerPollingInterval = 100.millis, miningCandidateCacheSize = cacheSize)
    )
    val initial = createUtxoState(settings)
    val txs = validTransactionsFromBoxHolder(initial._2, new RandomWrapper(Some(91)))
    val root = validFullBlock(None, initial._1, txs._1)
    var state = initial._1.applyModifier(root, None)(_ => ()).get
    var history = applyChain(HistoryTestHelpers.generateHistory(
      verifyTransactions = true, stateType = StateType.Utxo,
      PoPoWBootstrap = false, blocksToKeep = 100), Seq(root))
    val view = TestProbe()
    view.ignoreMsg {
      case _: org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.GetDataFromCurrentView[_, _] => true
    }
    val replies = TestProbe()
    val wallet = new ErgoWalletReader { val walletActor: ActorRef = system.deadLetters }
    val readers = system.actorOf(Props(new Actor {
      override def receive: Receive = {
        case GetReaders => sender() ! Readers(history, state, ErgoMemPool.empty(config), wallet)
      }
    }))
    val generator = CandidateGenerator(defaultMinerSecret.publicImage, readers, view.ref, config)
    val first = candidate()

    def candidate(forced: Boolean = false): Candidate = {
      generator.tell(GenerateCandidate(Seq.empty, reply = true, forced = forced), replies.ref)
      replies.expectMsgType[StatusReply[Candidate]](5.seconds).getValue
    }

    def accept(candidate: Candidate): AutolykosSolution = {
      pow.expectedParameters += candidate.candidateBlock.timestamp -> candidate.parameters.subBlocksPerBlock
      solution(candidate.candidateBlock.timestamp)
    }

    def submit(s: AutolykosSolution): StatusReply[Unit] = {
      generator.tell(InputSolutionFound(s), replies.ref)
      replies.expectMsgType[StatusReply[Unit]](2.seconds)
    }

    def next(): Candidate = {
      candidate(forced = true)
    }

    def applyInput(block: LocallyGeneratedInputBlock): Unit = {
      history.applyInputBlock(block.sbi) shouldBe None
      val (applied, _) = history.applyInputBlockTransactions(block.sbi.id, block.sbt.transactions, state)
      applied should contain(block.sbi.id)
      system.eventStream.publish(NewBestInputBlock(Some(block.sbi.id), local = true))
    }

    def close(): Unit = {
      TestKit.shutdownActorSystem(system)
      history.closeStorage()
      state.closeStorage()
    }
  }

  private def withFixture(test: Fixture => Unit): Unit = {
    val f = new Fixture
    try test(f) finally f.close()
  }

  it should "accept a late solution for previous work with that candidate's parameters" in withFixture { f =>
    val oldSolution = f.accept(f.first)
    val previous = f.state.stateContext
    val changed = new UtxoState(f.state.persistentProver, f.state.version, f.state.store, f.config) {
      override def withTransactions(txs: Seq[org.ergoplatform.modifiers.mempool.OutputsHolder]): UtxoState = this
      override def stateContext: ErgoStateContext = new ErgoStateContext(
        previous.lastHeaders, previous.lastExtensionOpt, previous.genesisStateDigest,
        previous.currentParameters.withNumOfSubblocksPerBlock(1),
        previous.validationSettings, previous.votingData)(previous.chainSettings)
    }
    f.generator.tell(ChangedState(changed), f.replies.ref)
    val current = f.next()
    current.parameters.subBlocksPerBlock should not be f.first.parameters.subBlocksPerBlock
    f.submit(oldSolution).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock].sbi.header.timestamp shouldBe f.first.candidateBlock.timestamp
    f.pow.checked.takeRight(2) shouldBe Vector(
      current.candidateBlock.timestamp -> current.parameters.subBlocksPerBlock,
      f.first.candidateBlock.timestamp -> f.first.parameters.subBlocksPerBlock)
  }

  it should "resume internal mining with fresh work after input application" in withFixture { f =>
    val miner = ErgoMiningThread(f.config, f.generator, defaultMinerSecret.w)(f.system)
    val first = f.view.expectMsgType[LocallyGeneratedInputBlock](8.seconds)
    f.generator.tell(GenerateCandidate(Seq.empty, reply = true, forced = false), f.replies.ref)
    f.replies.expectNoMessage(200.millis)
    f.applyInput(first)
    val second = f.view.expectMsgType[LocallyGeneratedInputBlock](8.seconds)
    second.sbi.header.timestamp should be > first.sbi.header.timestamp
    second.sbi.inputBlockFields.prevInputBlockId.map(_.toSeq) shouldBe Some(scorex.util.idToBytes(first.sbi.id).toSeq)
    f.system.stop(miner)
  }

  it should "reject input submissions while an ordering block is pending" in withFixture { f =>
    f.generator.tell(OrderingSolutionFound(f.accept(f.first)), f.replies.ref)
    f.replies.expectMsg(StatusReply.success(()))
    f.view.expectMsgType[LocallyGeneratedOrderingBlock]
    f.submit(solution(0)).isError shouldBe true
  }

  it should "preserve cached work after an invalid input submission" in withFixture { f =>
    f.submit(solution(0)).isError shouldBe true
    f.candidate().candidateBlock.timestamp shouldBe f.first.candidateBlock.timestamp
    f.submit(f.accept(f.first)).isSuccess shouldBe true
  }

  it should "reject duplicate submissions without applying the block twice" in withFixture { f =>
    val solved = f.accept(f.first)
    f.submit(solved).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock]
    f.submit(solved).isError shouldBe true
    f.view.expectNoMessage(200.millis)
  }

  it should "report unavailable work when no active or retained candidate exists" in withFixture { f =>
    // Enter the real initialized receive with no active/retained work and no pending
    // input. This exercises empty-cache completion, not the pending-input guard.
    val empty = CandidateGenerator.CandidateGeneratorState(
      None, None, f.history, f.state, ErgoMemPool.empty(f.config), 10.millis, None)
    val generator = f.system.actorOf(Props(new CandidateGenerator(
      defaultMinerSecret.publicImage, f.readers, f.view.ref, f.config) {
      override def preStart(): Unit = ()
      override def receive: Receive = {
        initialized(empty)
      }
    }))
    generator.tell(InputSolutionFound(solution(0)), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Unit]].getError.getMessage shouldBe
      "No cached candidate available"
    f.view.expectNoMessage(200.millis)
  }

  it should "reject older input work after an ordering parent change with a stale parent reason" in withFixture { f =>
    val oldSolution = f.accept(f.first)
    val nextTxs = validTransactionsFromBoxHolder(f.txs._2, new RandomWrapper(Some(92)))._1
    val block = validFullBlock(Some(f.root), f.state, nextTxs)
    f.state = f.state.applyModifier(block, None)(_ => ()).get
    f.history = applyChain(f.history, Seq(block))
    f.generator.tell(ChangedHistory(f.history), f.replies.ref)
    f.generator.tell(ChangedState(f.state), f.replies.ref)
    f.generator.tell(LocalBlockApplied(block.header, nextTxs.map(_.id)), f.replies.ref)
    f.candidate(forced = true).candidateBlock.parentOpt.map(_.id) shouldBe Some(block.id)
    val result = f.submit(oldSolution)
    result.isError shouldBe true
    result.getError.getMessage shouldBe "Stale input ordering parent"
    f.view.expectNoMessage(200.millis)
  }

  it should "resume internal mining when the holder never replies" in withFixture { f =>
    val miner = ErgoMiningThread(f.config, f.generator, defaultMinerSecret.w)(f.system)
    val first = f.view.expectMsgType[LocallyGeneratedInputBlock](8.seconds)
    val second = f.view.expectMsgType[LocallyGeneratedInputBlock](8.seconds)
    second.sbi.header.timestamp should be > first.sbi.header.timestamp
    f.system.stop(miner)
  }

  it should "time out the input barrier and refresh work without candidate polling" in withFixture { f =>
    val solved = f.accept(f.first)
    f.submit(solved).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock]
    // Unlike the internal miner test, no GenerateCandidate requests drive retries.
    f.replies.awaitAssert({
      f.submit(solution(0)).getError.getMessage shouldBe
        "No retained candidate matches input solution PoW"
    }, 8.seconds, 250.millis)
    f.candidate().candidateBlock.timestamp should be > f.first.candidateBlock.timestamp
  }

  it should "bound candidate deferrals across pending input turnover by the total request budget" in withFixture { f =>
    case class SetPending(id: scorex.util.ModifierId)
    case class FireDeferred(message: Any)
    val deferred = TestProbe()(f.system)
    val config = f.config.copy(nodeSettings = f.config.nodeSettings.copy(
      miningPendingInputMaxRetries = 2))
    val firstId = f.root.id
    val secondId = scorex.util.bytesToId(Array.fill[Byte](32)(2))
    val thirdId = scorex.util.bytesToId(Array.fill[Byte](32)(3))
    val initial = CandidateGenerator.CandidateGeneratorState(
      Some(f.first), None, f.history, f.state, ErgoMemPool.empty(config),
      10.millis, None, pendingInput = Some(firstId))
    val generator = f.system.actorOf(Props(new CandidateGenerator(
      defaultMinerSecret.publicImage, f.readers, f.view.ref, config) {
      private def behavior(id: scorex.util.ModifierId): Receive = {
        initialized(initial.copy(pendingInput = Some(id)))
      }
      override def preStart(): Unit = ()
      override def receive: Receive = behavior(firstId)
      override def aroundReceive(receive: Receive, message: Any): Unit = message match {
        case SetPending(id) => context.become(behavior(id))
        case FireDeferred(value) => super.aroundReceive(receive, value)
        case value: Product if value.productPrefix == "DeferredGenerateCandidate" =>
          deferred.ref ! value
        case _ => super.aroundReceive(receive, message)
      }
    }))
    generator.tell(GenerateCandidate(Seq.empty, reply = true, forced = false), f.replies.ref)
    val firstRetry = deferred.expectMsgType[Product]
    generator.tell(SetPending(secondId), f.replies.ref)
    generator.tell(FireDeferred(firstRetry), f.replies.ref)
    val secondRetry = deferred.expectMsgType[Product]
    secondRetry.productElement(1) shouldBe 2
    generator.tell(SetPending(thirdId), f.replies.ref)
    generator.tell(FireDeferred(secondRetry), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Candidate]].getValue shouldBe f.first
    deferred.expectNoMessage(200.millis)
  }

  it should "report an invalid solution error for null and preserve cached work" in withFixture { f =>
    f.generator.tell(InputSolutionFound(null), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Unit]].getError.getMessage shouldBe "Invalid mining solution"
    f.candidate().candidateBlock.timestamp shouldBe f.first.candidateBlock.timestamp
  }

  it should "complete retained ordering work while input application is pending" in withFixture { f =>
    val solved = f.accept(f.first)
    f.submit(solved).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock]
    f.generator.tell(OrderingSolutionFound(solved), f.replies.ref)
    f.replies.expectMsg(StatusReply.success(()))
    f.view.expectMsgType[LocallyGeneratedOrderingBlock]
  }

  Seq(false, true).foreach { competing =>
    val appliedWork = if (competing) "a competing ordering block" else "the solved ordering block"
    it should s"resume mining after $appliedWork applies behind an input barrier" in withFixture { f =>
      f.accept(f.first)
      // This block becomes a parent: zero distance would imply an unbounded
      // interlink level for the fixture's version-one header.
      val solved = new AutolykosSolution(
        defaultMinerSecret.publicImage.value, defaultMinerSecret.publicImage.value,
        Longs.toByteArray(f.first.candidateBlock.timestamp), org.ergoplatform.mining.q / 2)
      f.submit(solved).isSuccess shouldBe true
      val input = f.view.expectMsgType[LocallyGeneratedInputBlock]
      f.generator.tell(OrderingSolutionFound(solved), f.replies.ref)
      f.replies.expectMsg(StatusReply.success(()))
      val ordering = f.view.expectMsgType[LocallyGeneratedOrderingBlock].efb

      f.applyInput(input)
      // Wait until the applied input event has cleared the barrier and refreshed work.
      f.candidate().candidateBlock.inputBlockFields.prevInputBlockId.map(_.toSeq) shouldBe
        Some(scorex.util.idToBytes(input.sbi.id).toSeq)
      f.generator.tell(OrderingSolutionFound(solved), f.replies.ref)
      f.replies.expectMsgType[StatusReply[Unit]].getError.getMessage should
        startWith("Block already solved")

      val applied = if (competing) {
        val txs = validTransactionsFromBoxHolder(f.txs._2, new RandomWrapper(Some(92)))._1
        validFullBlock(Some(f.root), f.state, txs)
      } else ordering
      applied.height shouldBe ordering.height
      if (competing) applied.id should not be ordering.id
      f.state = f.state.applyModifier(applied, None)(_ => ()).get
      f.history = applyChain(f.history, Seq(applied))
      f.generator.tell(ChangedHistory(f.history), f.replies.ref)
      f.generator.tell(ChangedState(f.state), f.replies.ref)
      f.generator.tell(LocalBlockApplied(applied.header, applied.transactions.map(_.id)), f.replies.ref)

      val next = f.candidate()
      next.candidateBlock.parentOpt.map(_.id) shouldBe Some(applied.id)
      f.generator.tell(OrderingSolutionFound(f.accept(next)), f.replies.ref)
      f.replies.expectMsg(StatusReply.success(()))
      f.view.expectMsgType[LocallyGeneratedOrderingBlock].efb.parentId shouldBe applied.id
    }
  }

  it should "reject retained ordering work after history advances" in withFixture { f =>
    val solved = f.accept(f.first)
    val txs = validTransactionsFromBoxHolder(f.txs._2, new RandomWrapper(Some(92)))._1
    val block = validFullBlock(Some(f.root), f.state, txs)
    f.history = applyChain(f.history, Seq(block))
    f.generator.tell(ChangedHistory(f.history), f.replies.ref)
    f.generator.tell(OrderingSolutionFound(solved), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Unit]].getError.getMessage shouldBe "Stale ordering parent"
    f.view.expectNoMessage(200.millis)
  }

  it should "reject old ordering work after peer application and accept current work" in withFixture { f =>
    val oldSolution = f.accept(f.first)
    val txs = validTransactionsFromBoxHolder(f.txs._2, new RandomWrapper(Some(92)))._1
    val block = validFullBlock(Some(f.root), f.state, txs)
    f.state = f.state.applyModifier(block, None)(_ => ()).get
    f.history = applyChain(f.history, Seq(block))
    f.generator.tell(ChangedHistory(f.history), f.replies.ref)
    f.generator.tell(ChangedState(f.state), f.replies.ref)
    f.generator.tell(LocalBlockApplied(block.header, txs.map(_.id)), f.replies.ref)
    val current = f.candidate()
    current.candidateBlock.parentOpt.map(_.id) shouldBe Some(block.id)
    f.generator.tell(OrderingSolutionFound(oldSolution), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Unit]].getError.getMessage shouldBe "Stale ordering parent"
    f.view.expectNoMessage(200.millis)
    (f.candidate() eq current) shouldBe true
    val currentSolution = f.accept(current)
    f.generator.tell(OrderingSolutionFound(currentSolution), f.replies.ref)
    f.replies.expectMsg(StatusReply.success(()))
    f.view.expectMsgType[LocallyGeneratedOrderingBlock].efb.id shouldBe
      CandidateGenerator.completeOrderingBlock(current.candidateBlock, currentSolution).id
  }

  it should "reject superseded input work while preserving and accepting current work" in withFixture { f =>
    val oldSolution = f.accept(f.first)
    f.submit(oldSolution).isSuccess shouldBe true
    f.applyInput(f.view.expectMsgType[LocallyGeneratedInputBlock])
    val current = f.candidate()
    current.candidateBlock.inputBlockFields.prevInputBlockId.map(scorex.util.bytesToId) shouldBe
      f.history.bestBlocks._2.map(_.id)
    // Different distance yields an unknown sibling header for the same old work.
    val sibling = new AutolykosSolution(oldSolution.pk, oldSolution.w, oldSolution.n, BigInt(1))
    f.submit(sibling).getError.getMessage shouldBe "Stale input parent"
    f.view.expectNoMessage(200.millis)
    (f.candidate() eq current) shouldBe true
    val currentSolution = f.accept(current)
    f.submit(currentSolution).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock].sbi.id shouldBe
      CandidateGenerator.completeInputBlock(current.candidateBlock, currentSolution)._1.id
  }

  it should "preserve cached work when a remote input event has no new best tip" in withFixture { f =>
    f.generator.tell(NewBestInputBlock(None, local = false), f.replies.ref)
    f.candidate().candidateBlock.timestamp shouldBe f.first.candidateBlock.timestamp
  }

  it should "preserve cached work when the announced input tip is already current" in withFixture { f =>
    f.submit(f.accept(f.first)).isSuccess shouldBe true
    val input = f.view.expectMsgType[LocallyGeneratedInputBlock]
    f.applyInput(input)
    val current = f.candidate()
    f.generator.tell(NewBestInputBlock(Some(input.sbi.id), local = false), f.replies.ref)
    f.candidate().candidateBlock.timestamp shouldBe current.candidateBlock.timestamp
  }

  it should "report distinct input errors for unmatched work and pending application" in withFixture { f =>
    f.submit(solution(0)).getError.getMessage shouldBe "No retained candidate matches input solution PoW"
    f.submit(f.accept(f.first)).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock]
    f.submit(solution(0)).getError.getMessage should startWith("Input block pending application")
  }

  it should "reject invalid ordering solutions after candidate replacement without forwarding" in withFixture { f =>
    f.next()
    f.generator.tell(OrderingSolutionFound(solution(0)), f.replies.ref)
    f.replies.expectMsgType[StatusReply[Unit]].isError shouldBe true
    f.view.expectNoMessage(200.millis)
  }

  it should "keep the oldest candidate usable after two replacements with the default cache size" in withFixture { f =>
    val solved = f.accept(f.first)
    f.next()
    f.next()
    f.submit(solved).isSuccess shouldBe true
  }

  it should "evict older work from a full cache and keep active work usable" in {
    val f = new Fixture(cacheSize = 1)
    try {
      val oldSolution = f.accept(f.first)
      val current = f.next()
      f.submit(oldSolution).isError shouldBe true
      f.candidate().candidateBlock.timestamp shouldBe current.candidateBlock.timestamp
      f.submit(f.accept(current)).isSuccess shouldBe true
    } finally f.close()
  }

  it should "refresh work after holder processing without a best input event" in withFixture { f =>
    import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.GetDataFromCurrentView
    import org.ergoplatform.nodeView.ErgoNodeViewHolder.CurrentView
    f.view.ignoreNoMsg()
    f.submit(f.accept(f.first)).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock]
    val barrier = f.view.expectMsgType[GetDataFromCurrentView[UtxoState, Any]]
    // A losing fork or a rejected input completes processing without changing the best tip.
    val response = barrier.f(CurrentView(f.history, f.state, null, ErgoMemPool.empty(f.config)))
    f.generator.tell(response, f.view.ref)
    val next = f.candidate()
    next.candidateBlock.timestamp should be > f.first.candidateBlock.timestamp
    f.submit(f.accept(next)).isSuccess shouldBe true
  }

  it should "reject duplicates after input application and preserve refreshed work" in withFixture { f =>
    val solved = f.accept(f.first)
    f.submit(solved).isSuccess shouldBe true
    f.applyInput(f.view.expectMsgType[LocallyGeneratedInputBlock])
    val next = f.candidate()
    next.candidateBlock.timestamp should be > f.first.candidateBlock.timestamp
    f.submit(solved).isError shouldBe true
    f.view.expectNoMessage(200.millis)
    f.candidate().candidateBlock.timestamp shouldBe next.candidateBlock.timestamp
  }

  it should "match real input PoW to retained work using its source parameters" in withFixture { f =>
    val realPow = settings.chainSettings.powScheme
    val older = f.first.copy(candidateBlock = f.first.candidateBlock.copy(
      nBits = org.ergoplatform.mining.difficulty.DifficultySerializer.encodeCompactBits(BigInt(128))),
      parameters = f.first.parameters.withNumOfSubblocksPerBlock(64))
    val current = older.copy(candidateBlock = older.candidateBlock.copy(
      timestamp = older.candidateBlock.timestamp + 1),
      parameters = older.parameters.withNumOfSubblocksPerBlock(1))
    val solved = (0L until 10000L).iterator.map(solution).find { s =>
      val oldHeader = CandidateGenerator.completeInputBlock(older.candidateBlock, s)._1.header
      val newHeader = CandidateGenerator.completeInputBlock(current.candidateBlock, s)._1.header
      realPow.checkInputBlockPoW(oldHeader, older.parameters) &&
        !realPow.checkInputBlockPoW(oldHeader, current.parameters) &&
        !realPow.checkInputBlockPoW(newHeader, current.parameters)
    }.getOrElse(fail("No discriminating real PoW found in 10000 nonces"))
    val completed = CandidateGenerator.completeMatchingInputBlock(
      Seq(current, older), solved, realPow).get
    completed._1.header.timestamp shouldBe older.candidateBlock.timestamp
    realPow.checkInputBlockPoW(completed._1.header, older.parameters) shouldBe true
  }


  it should "produce a distinct input block for a nonce reused after input application" in withFixture { f =>
    // Low-difficulty miners often solve successive candidates with nonce zero. Without a
    // work id, duplicate detection must use the completed header, not the raw solution.
    f.pow.acceptEveryNonce = true
    val reused = solution(0)
    f.submit(reused).isSuccess shouldBe true
    val first = f.view.expectMsgType[LocallyGeneratedInputBlock]
    f.applyInput(first)
    f.candidate().candidateBlock.timestamp should be > first.sbi.header.timestamp
    f.submit(reused).isSuccess shouldBe true
    f.view.expectMsgType[LocallyGeneratedInputBlock].sbi.id should not be first.sbi.id
  }

}
