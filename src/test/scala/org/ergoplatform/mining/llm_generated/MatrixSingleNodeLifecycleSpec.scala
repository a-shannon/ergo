package org.ergoplatform.mining

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{StatusReply, ask}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.google.common.io.Files.createTempDir
import org.ergoplatform.mining.ErgoMiner.StartMining
import org.ergoplatform.mining.CandidateGenerator.{Candidate, GenerateCandidate}
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction, UnsignedErgoTransaction}
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.FullBlockApplied
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.LocallyGeneratedTransaction
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.state.{StateType, UtxoStateReader}
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef}
import org.ergoplatform.settings.NetworkType.DevNet60
import org.ergoplatform.settings.{ErgoSettings, ErgoSettingsReader}
import org.ergoplatform.utils.ErgoTestHelpers
import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, ErgoTreePredef, Input, InputSolutionFound, OrderingSolutionFound}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.encode.Base16
import sigma.serialization.ErgoTreeSerializer

import scala.concurrent.duration._

class MatrixSingleNodeLifecycleSpec extends AnyFlatSpec with Matchers with ErgoTestHelpers with Eventually {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  implicit private val timeout: Timeout = defaultTimeout

  private val candidateGenDelay = 3.seconds
  private val blockValidationDelay = 2.seconds
  private val lifecycleDelay = 30.seconds

  "A single Matrix node" should "admit transactions, process an input block, and finalize them in an ordering block" in new TestKit(
    ActorSystem()
  ) {
    try {
    val replyProbe = new TestProbe(system)
    val blockProbe = new TestProbe(system)
    system.eventStream.subscribe(blockProbe.ref, classOf[FullBlockApplied])

    val baseSettings = ErgoSettingsReader.read()
    val settings: ErgoSettings = baseSettings.copy(
      networkType = DevNet60,
      directory = createTempDir().getAbsolutePath,
      nodeSettings = baseSettings.nodeSettings.copy(
        mining = true,
        stateType = StateType.Utxo,
        useExternalMiner = true,
        internalMinersCount = 0,
        miningPubKeyHex = Some(Base16.encode(groupElemToBytes(defaultMinerPk.value))),
        internalMinerPollingInterval = 1.second,
        offlineGeneration = true,
        verifyTransactions = true
      ),
      chainSettings = baseSettings.chainSettings.copy(
        blockInterval = 1.second,
        initialDifficultyHex = "0080",
        powScheme = new AutolykosPowScheme(
          baseSettings.chainSettings.powScheme.k,
          baseSettings.chainSettings.powScheme.n
        )
      )
    )
    val powScheme = settings.chainSettings.powScheme

    def readers(readersHolderRef: ActorRef): Readers =
      await((readersHolderRef ? GetReaders).mapTo[Readers])

    def mine(candidate: Candidate, inputBlock: Boolean): ErgoFullBlock = {
      val deadline = lifecycleDelay.fromNow
      var nonce = 0L
      while (nonce < 100000L && deadline.hasTimeLeft()) {
        powScheme.proveCandidate(
          candidate.candidateBlock,
          defaultMinerSecret.w,
          nonce,
          nonce + 1L,
          candidate.parameters
        ) match {
          case org.ergoplatform.OrderingBlockFound(block) if !inputBlock => return block
          case org.ergoplatform.InputBlockFound(block) if inputBlock => return block
          case _ => ()
        }
        nonce += 1L
      }
      fail(s"No ${if (inputBlock) "input" else "ordering"} solution within $nonce nonces")
    }

    def expectOrderingBlockApplied(block: ErgoFullBlock): Unit = {
      replyProbe.expectMsg(blockValidationDelay, StatusReply.Success(()))
      blockProbe.fishForMessage(lifecycleDelay) {
        case FullBlockApplied(header) => header.id == block.header.id
        case _ => false
      }
    }

    val viewHolderRef = ErgoNodeViewRef(settings)
    val readersHolderRef = ErgoReadersHolderRef(viewHolderRef)
    val miner = ErgoMiner(
      settings,
      viewHolderRef,
      readersHolderRef,
      Some(defaultMinerSecret)
    )
    miner ! StartMining

    def requestCandidate(forced: Boolean): Candidate = eventually(timeout(candidateGenDelay), interval(100.millis)) {
      await(
        miner
          .askWithStatus(GenerateCandidate(Seq.empty, reply = true, forced = forced, optPk = None))
          .mapTo[Candidate]
      )
    }

    val initialReaders = readers(readersHolderRef)
    val startHeader = initialReaders.h.bestHeaderOpt

    val fundingCandidate = requestCandidate(forced = false)
    val fundingBlock = mine(fundingCandidate, inputBlock = false)
    miner.tell(OrderingSolutionFound(fundingBlock.header.powSolution), replyProbe.ref)
    expectOrderingBlockApplied(fundingBlock)

    val fundedReaders = readers(readersHolderRef)
    val rewardBox: ErgoBox = fundedReaders.h.bestFullBlockOpt.get.transactions.last.outputs.last
    rewardBox.propositionBytes shouldBe ErgoTreePredef
      .rewardOutputScript(emission.settings.minerRewardDelay, defaultMinerPk)
      .bytes

    // sigmaProp(Global.serialize(2).size > 0) requires the version-6 interpreter.
    // CandidateGenerator must classify these transactions into the Matrix input payload.
    val version6Tree = ErgoTreeSerializer.DefaultSerializer.deserializeErgoTree(
      Base16.decode("1b110204040400d191b1dc6a03dd0173007301").get
    )
    val fee = 1000000L
    val feeTree = ErgoTreePredef.feeProposition(settings.chainSettings.monetary.minerRewardDelay)
    def feeOutput: ErgoBoxCandidate =
      new ErgoBoxCandidate(fee, feeTree, fundedReaders.s.stateContext.currentHeight)
    val firstUnsigned = new UnsignedErgoTransaction(
      IndexedSeq(Input(rewardBox.id, emptyProverResult)),
      IndexedSeq.empty,
      IndexedSeq(new ErgoBoxCandidate(rewardBox.value - fee, version6Tree,
        fundedReaders.s.stateContext.currentHeight), feeOutput)
    )
    val firstTx = ErgoTransaction(
      defaultProver.sign(firstUnsigned, IndexedSeq(rewardBox), IndexedSeq.empty, fundedReaders.s.stateContext).get
    )
    val intermediateBox = firstTx.outputs.head
    val secondTx = firstTx.copy(
      inputs = IndexedSeq(Input(intermediateBox.id, emptyProverResult)),
      outputCandidates = IndexedSeq(new ErgoBoxCandidate(
        intermediateBox.value - fee,
        version6Tree,
        intermediateBox.creationHeight,
        intermediateBox.additionalTokens,
        intermediateBox.additionalRegisters
      ), feeOutput)
    )

    viewHolderRef ! LocallyGeneratedTransaction(UnconfirmedTransaction(firstTx, None))
    eventually(timeout(candidateGenDelay), interval(100.millis)) {
      readers(readersHolderRef).m.contains(firstTx.id) shouldBe true
    }
    viewHolderRef ! LocallyGeneratedTransaction(UnconfirmedTransaction(secondTx, None))
    eventually(timeout(candidateGenDelay), interval(100.millis)) {
      val mempool = readers(readersHolderRef).m
      mempool.contains(firstTx.id) shouldBe true
      mempool.contains(secondTx.id) shouldBe true
      mempool.getAllPrioritized.map(_.id) should contain theSameElementsInOrderAs Seq(firstTx.id, secondTx.id)
    }

    val inputCandidate = requestCandidate(forced = true)
    inputCandidate.candidateBlock.version shouldBe Header.Interpreter60Version
    inputCandidate.candidateBlock.transactions.map(_.id) should not contain firstTx.id
    inputCandidate.candidateBlock.transactions.map(_.id) should not contain secondTx.id
    inputCandidate.candidateBlock.inputBlockTransactions.map(_.id) should contain theSameElementsInOrderAs
      Seq(firstTx.id, secondTx.id)

    val inputBlock = mine(inputCandidate, inputBlock = true)
    miner.tell(InputSolutionFound(inputBlock.header.powSolution), replyProbe.ref)
    replyProbe.expectMsg(blockValidationDelay, StatusReply.Success(()))

    eventually(timeout(lifecycleDelay), interval(100.millis)) {
      val afterInput = readers(readersHolderRef)
      afterInput.h.bestInputBlock().map(_.id) shouldBe Some(inputBlock.id)
      afterInput.h.getBestOrderingCollectedInputBlocksTransactions().map(_.id) should contain theSameElementsInOrderAs
        Seq(firstTx.id, secondTx.id)
      afterInput.m.contains(firstTx.id) shouldBe false
      afterInput.m.contains(secondTx.id) shouldBe false
      val confirmedState = afterInput.s.asInstanceOf[UtxoStateReader]
      confirmedState.boxById(rewardBox.id) shouldBe Some(rewardBox)
      secondTx.outputs.foreach(output => confirmedState.boxById(output.id) shouldBe None)
    }

    val orderingCandidate = requestCandidate(forced = true)
    orderingCandidate.candidateBlock.transactions.map(_.id) should contain allOf (firstTx.id, secondTx.id)
    orderingCandidate.candidateBlock.inputBlockTransactions shouldBe empty
    val feeBoxes = Seq(firstTx.outputs.last, secondTx.outputs.last)
    val feeIds = feeBoxes.map(box => Base16.encode(box.id)).toSet
    val collectors = orderingCandidate.candidateBlock.orderingBlockTransactions.filter { tx =>
      tx.inputs.exists(input => feeIds.contains(Base16.encode(input.boxId)))
    }
    collectors.size shouldBe 1
    val collector = collectors.head
    collector.inputs.map(input => Base16.encode(input.boxId)).toSet shouldBe feeIds
    collector.outputs.map(_.value).sum shouldBe 2 * fee
    collector.outputs.head.propositionBytes shouldBe ErgoTreePredef.rewardOutputScript(
      settings.chainSettings.monetary.minerRewardDelay, defaultMinerPk).bytes

    val orderingBlock = mine(orderingCandidate, inputBlock = false)
    miner.tell(OrderingSolutionFound(orderingBlock.header.powSolution), replyProbe.ref)
    expectOrderingBlockApplied(orderingBlock)

    val finalizedReaders = readers(readersHolderRef)
    val finalizedState = finalizedReaders.s.asInstanceOf[UtxoStateReader]
    finalizedState.boxById(rewardBox.id) shouldBe None
    finalizedState.boxById(intermediateBox.id) shouldBe None
    finalizedState.boxById(secondTx.outputs.head.id) shouldBe Some(secondTx.outputs.head)
    feeBoxes.foreach(box => finalizedState.boxById(box.id) shouldBe None)
    collector.outputs.foreach(output => finalizedState.boxById(output.id) shouldBe Some(output))
    finalizedReaders.h
      .chainToHeader(startHeader, finalizedReaders.h.bestHeaderOpt.get)
      ._2
      .headers
      .flatMap(finalizedReaders.h.getFullBlock)
      .flatMap(_.blockTransactions.txs)
      .map(_.id) should contain allOf (firstTx.id, secondTx.id)
    } finally {
      TestKit.shutdownActorSystem(system)
    }
  }
}
