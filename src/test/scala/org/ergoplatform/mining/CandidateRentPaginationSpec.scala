package org.ergoplatform.mining

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.StatusReply
import akka.testkit.{TestKit, TestProbe}
import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.mining.CandidateGenerator.{Candidate, GenerateCandidate}
import org.ergoplatform.modifiers.history.HeaderChain
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.history.ErgoHistoryReader
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, IndexedErgoBox, NumericBoxIndex, StorageRentBox}
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.nodeView.state.{BoxHolder, ErgoStateContext, UtxoState, VotingData}
import org.ergoplatform.nodeView.wallet.ErgoWalletReader
import org.ergoplatform.settings.Constants
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.utils.TestFileUtils
import scorex.util.bytesToId

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration._
import sigmastate.helpers.TestingHelpers._

/** A real rent-index scan must make progress even when its first page cannot be claimed. */
class CandidateRentPaginationSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}
  import org.ergoplatform.utils.generators.ErgoCoreTransactionGenerators._

  private class FixedReadersHolder(readers: Readers) extends Actor {
    override def receive: Receive = { case GetReaders => sender() ! readers }
  }

  property("a claimable row after 400 filtered rows is reached without deleting unspent rows") {
    new TestKit(ActorSystem()) {
      val settings = baseSettings.copy(
        directory = createTempDir.getAbsolutePath,
        nodeSettings = baseSettings.nodeSettings.copy(
          extraIndex = true, storageRentCollection = true,
          blockCandidateGenerationInterval = 1.second))
      val height = 3 * Constants.StoragePeriod
      val threshold = height - Constants.StoragePeriod
      val filtered = (0 until 400).map { i =>
        testBox(1L, Constants.TrueTree, threshold - 400 + i, Seq.empty, Map.empty)
      }
      val claimable = testBox(10000000000L, Constants.TrueTree, threshold,
        Seq.empty, Map.empty)
      val witnessBox = testBox(10000000000L, Constants.TrueTree, height - 2,
        Seq.empty, Map.empty)
      val witness = ErgoTransaction(
        IndexedSeq(Input(witnessBox.id, emptyProverResult)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(witnessBox.value, Constants.TrueTree, height)))
      val baseState = UtxoState.fromBoxHolder(
        BoxHolder(filtered ++ Seq(claimable, witnessBox)), None,
        createTempDir, settings, parameters)
      val block = invalidErgoFullBlockGen.sample.get
      val parentHeader = block.header.copy(height = height - 1)
      val parentBlock = block.copy(parentHeader)
      val agedContext = new ErgoStateContext(Seq(parentHeader), None,
        genesisStateDigest, parameters, validationSettingsNoIl,
        VotingData.empty)(settings.chainSettings)
      val state = new UtxoState(baseState.persistentProver, baseState.version,
        baseState.store, settings) {
        override def stateContext: ErgoStateContext = agedContext
        override def emissionBoxOpt: Option[ErgoBox] = None
      }
      val storage = HistoryStorage(settings)
      (filtered :+ claimable).zipWithIndex.foreach { case (box, index) =>
        val indexed = new IndexedErgoBox(box.creationHeight, None, None, None,
          box, index.toLong)
        storage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
          Array[ExtraIndex](indexed, NumericBoxIndex(index.toLong, indexed.id),
            StorageRentBox(indexed)))
      }
      val chainAvailable = new AtomicBoolean(true)
      val failNextPage = new AtomicBoolean(false)
      val history = Proxy.newProxyInstance(
        classOf[ErgoHistoryReader].getClassLoader,
        Array[Class[_]](classOf[ErgoHistoryReader]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
            method.getName match {
              case "lastHeaders$default$2" => Int.box(0)
              case "lastHeaders" => HeaderChain.empty
              case "bestFullBlockOpt" =>
                if (chainAvailable.get()) Some(parentBlock) else None
              case "bestFullBlockAt" => Some(parentBlock)
              case "isInSelectedFullChain" =>
                java.lang.Boolean.valueOf(args(0) == parentBlock.id)
              case "storageRentBoxesAtOrBefore" =>
                storage.storageRentBoxesAtOrBefore(
                  args(0).asInstanceOf[Int], args(1).asInstanceOf[Int])
              case "storageRentBoxesPage" =>
                if (failNextPage.compareAndSet(true, false))
                  throw new IllegalStateException("temporary rent-index read failure")
                storage.storageRentBoxesPage(args(0).asInstanceOf[Int],
                  args(1).asInstanceOf[Int], args(2).asInstanceOf[Option[Vector[Byte]]])
              case "typedExtraIndexById" =>
                storage.getExtraIndex(args(0).asInstanceOf[scorex.util.ModifierId])
              case "typedModifierById" => None
              case "requiredDifficultyAfter" => parentHeader.requiredDifficulty
              case other => throw new UnsupportedOperationException(s"unexpected history read: $other")
            }
        }).asInstanceOf[ErgoHistoryReader]
      val wallet = new ErgoWalletReader {
        val walletActor: ActorRef = system.deadLetters
      }
      val readers = Readers(history, state, ErgoMemPool.empty(settings), wallet)
      val readersHolder = system.actorOf(Props(new FixedReadersHolder(readers)))
      val generator = CandidateGenerator(defaultMinerPk, readersHolder,
        system.deadLetters, settings)
      val reply = TestProbe()

      try {
        val candidates = (1 to 3).map { _ =>
          generator.tell(GenerateCandidate(Seq(witness), reply = true, forced = true), reply.ref)
          reply.expectMsgPF(10.seconds) {
            case StatusReply.Success(candidate: Candidate) => candidate
          }
        }
        candidates.exists(_.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(claimable.id)))) shouldBe true
        storage.storageRentBoxesAtOrBefore(threshold, 402).length shouldBe 401

        reply.watch(generator)
        system.stop(generator)
        reply.expectTerminated(generator, 10.seconds)
        val cachedGenerator = CandidateGenerator(defaultMinerPk, readersHolder,
          system.deadLetters, settings)
        cachedGenerator.tell(GenerateCandidate(Seq(witness), reply = true, forced = false),
          reply.ref)
        val first = reply.expectMsgPF(10.seconds) {
          case StatusReply.Success(candidate: Candidate) => candidate
        }
        first.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(claimable.id))) shouldBe false
        // Background pages discover the later row. The next normal request must bypass
        // the old cache, while the issued candidate remains usable until that request.
        reply.expectNoMessage(2.seconds)
        cachedGenerator.tell(GenerateCandidate(Seq(witness), reply = true, forced = false),
          reply.ref)
        val refreshed = reply.expectMsgPF(10.seconds) {
          case StatusReply.Success(candidate: Candidate) => candidate
        }
        refreshed.candidateBlock.transactions.exists(
          _.inputs.exists(_.boxId.sameElements(claimable.id))) shouldBe true
        storage.storageRentBoxesAtOrBefore(threshold, 402).length shouldBe 401

        reply.watch(cachedGenerator)
        system.stop(cachedGenerator)
        reply.expectTerminated(cachedGenerator, 10.seconds)
        val rentOnlyGenerator = CandidateGenerator(defaultMinerPk, readersHolder,
          system.deadLetters, settings)
        reply.expectNoMessage(2.seconds)
        rentOnlyGenerator.tell(GenerateCandidate(Seq.empty, reply = true, forced = false),
          reply.ref)
        val rentOnly = reply.expectMsgPF(10.seconds) {
          case StatusReply.Success(candidate: Candidate) => candidate
        }
        rentOnly.candidateBlock.transactions.size shouldBe 1
        rentOnly.candidateBlock.transactions.head.inputs.exists(
          _.boxId.sameElements(claimable.id)) shouldBe true
        reply.watch(rentOnlyGenerator)
        system.stop(rentOnlyGenerator)
        reply.expectTerminated(rentOnlyGenerator, 10.seconds)

        val (_, firstProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings, CandidateGenerator.RentScanState())
        firstProgress.state.after should not be None

        val queued = firstProgress.state.copy(
          pendingClaims = Seq.fill(4)(Seq(bytesToId(claimable.id))))
        val (boundedAttempt, boundedProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings, queued)
        val boundedCandidate = boundedAttempt.get.get._1
        boundedCandidate.candidateBlock.transactions.size shouldBe 2
        val boundedClaims = boundedCandidate.candidateBlock.transactions.filter(
          _.inputs.exists(_.boxId.sameElements(claimable.id)))
        boundedClaims.size shouldBe 1
        boundedClaims.head.inputs.size should be <= StorageRentClaimBuilder.MaxClaims
        boundedProgress.state.after shouldBe firstProgress.state.after
        boundedProgress.state.pendingClaims shouldBe empty
        val (_, continued) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings, boundedProgress.state)
        continued.state.after should not be firstProgress.state.after
        val otherTip = bytesToId(Array.fill(32)(42.toByte))
        val (_, reorged) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings,
          firstProgress.state.copy(tip = Some((height - 1, otherTip))))
        reorged.state.after shouldBe firstProgress.state.after
        storage.storageRentBoxesAtOrBefore(threshold, 402).length shouldBe 401

        // The extra index can catch up after a candidate was cached. An empty
        // sweep must pause, then discover the newly indexed box without a new block.
        val lateStorage = HistoryStorage(settings.copy(directory = createTempDir.getAbsolutePath))
        val latePageReads = new AtomicInteger()
        val failNextLatePage = new AtomicBoolean(false)
        val lateHistory = Proxy.newProxyInstance(
          classOf[ErgoHistoryReader].getClassLoader,
          Array[Class[_]](classOf[ErgoHistoryReader]),
          new InvocationHandler {
            override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
              method.getName match {
                case "storageRentBoxesPage" =>
                  latePageReads.incrementAndGet()
                  if (failNextLatePage.compareAndSet(true, false))
                    throw new IllegalStateException("temporary background rent-index failure")
                  lateStorage.storageRentBoxesPage(args(0).asInstanceOf[Int],
                    args(1).asInstanceOf[Int], args(2).asInstanceOf[Option[Vector[Byte]]])
                case "typedExtraIndexById" =>
                  lateStorage.getExtraIndex(args(0).asInstanceOf[scorex.util.ModifierId])
                case _ =>
                  Proxy.getInvocationHandler(history).invoke(history, method, args)
              }
          }).asInstanceOf[ErgoHistoryReader]
        val lateReaders = Readers(lateHistory, state, ErgoMemPool.empty(settings), wallet)
        val lateHolder = system.actorOf(Props(new FixedReadersHolder(lateReaders)))
        val lateGenerator = CandidateGenerator(defaultMinerPk, lateHolder,
          system.deadLetters, settings)
        try {
          lateGenerator.tell(GenerateCandidate(Seq(witness), reply = true, forced = false),
            reply.ref)
          val beforeCatchup = reply.expectMsgPF(10.seconds) {
            case StatusReply.Success(candidate: Candidate) => candidate
          }
          beforeCatchup.candidateBlock.transactions.size shouldBe 1
          val initialReads = latePageReads.get()
          failNextLatePage.set(true)
          reply.expectNoMessage(1.second)
          latePageReads.get() shouldBe initialReads

          // The first background page fails once. The actor must keep its
          // bounded timer armed so a later index catch-up can still be seen.
          reply.expectNoMessage(7.seconds)
          val failedReads = latePageReads.get()
          failedReads should be > initialReads

          val indexed = new IndexedErgoBox(claimable.creationHeight, None, None, None,
            claimable, 0L)
          lateStorage.insertExtra(Array.empty[(Array[Byte], Array[Byte])],
            Array[ExtraIndex](indexed, NumericBoxIndex(0L, indexed.id),
              StorageRentBox(indexed)))
          reply.expectNoMessage(6.seconds)
          latePageReads.get() should be > failedReads
          lateGenerator.tell(GenerateCandidate(Seq(witness), reply = true, forced = false),
            reply.ref)
          val afterCatchup = reply.expectMsgPF(10.seconds) {
            case StatusReply.Success(candidate: Candidate) => candidate
          }
          afterCatchup.candidateBlock.transactions.exists(
            _.inputs.exists(_.boxId.sameElements(claimable.id))) shouldBe true
        } finally {
          reply.watch(lateGenerator)
          system.stop(lateGenerator)
          reply.expectTerminated(lateGenerator, 10.seconds)
          val stoppedReads = latePageReads.get()
          reply.expectNoMessage(6.seconds)
          latePageReads.get() shouldBe stoppedReads
          lateStorage.close()
        }

        // A background pass must stay scheduled across transient chain or index gaps.
        chainAvailable.set(false)
        val (unsynced, unsyncedProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings, firstProgress.state, rentScanOnly = true)
        unsynced shouldBe None
        unsyncedProgress.more shouldBe true
        unsyncedProgress.wrapped shouldBe true
        chainAvailable.set(true)
        failNextPage.set(true)
        val (failedPage, failedProgress) = CandidateGenerator.generateCandidateWithRentScan(
          history, state, ErgoMemPool.empty(settings), defaultMinerPk,
          Seq(witness), None, settings, firstProgress.state, rentScanOnly = true)
        failedPage.get.isFailure shouldBe true
        failedProgress.more shouldBe true
        failedProgress.wrapped shouldBe true
      } finally {
        try TestKit.shutdownActorSystem(system)
        finally {
          storage.close()
          state.closeStorage()
        }
      }
    }
  }
}
