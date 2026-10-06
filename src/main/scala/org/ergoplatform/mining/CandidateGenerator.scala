package org.ergoplatform.mining

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import akka.pattern.StatusReply
import com.google.common.primitives.Longs
import org.ergoplatform.ErgoBox.TokenId
import org.ergoplatform.mining.AutolykosPowScheme.derivedHeaderFields
import org.ergoplatform.mining.difficulty.DifficultySerializer
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.history._
import org.ergoplatform.modifiers.history.extension.Extension
import org.ergoplatform.modifiers.history.header.{Header, HeaderWithoutPow}
import org.ergoplatform.modifiers.history.popow.NipopowAlgos
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages._
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.EliminateTransactions
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.LocallyGeneratedModifier
import org.ergoplatform.nodeView.history.ErgoHistoryUtils.Height
import org.ergoplatform.nodeView.history.{ErgoHistoryReader, ErgoHistoryUtils}
import org.ergoplatform.nodeView.mempool.ErgoMemPoolReader
import org.ergoplatform.nodeView.state.{ErgoState, ErgoStateContext, StateType, UtxoStateReader}
import org.ergoplatform.settings.{Constants, ErgoSettings, ErgoValidationSettingsUpdate, Parameters}
import org.ergoplatform.sdk.wallet.Constants.MaxAssetsPerBox
import org.ergoplatform.wallet.interpreter.ErgoInterpreter
import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, ErgoTreePredef, Input}
import scorex.crypto.authds.{ADDigest, ADKey, SerializedAdProof}
import scorex.crypto.hash.Digest32
import scorex.util.encode.Base16
import scorex.util.{ModifierId, ScorexLogging, bytesToId, idToBytes}
import sigma.ast.syntax.ErgoBoxRType
import sigma.Extensions.ArrayOps
import sigma.crypto.CryptoFacade
import sigma.data.{Digest32Coll, ProveDlog}
import sigma.interpreter.ProverResult
import sigma.validation.ReplacedRule
import sigma.{Coll, Colls}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

/** Responsible for generating block candidates and validating solutions.
  * It is observing changes of history, utxo state, mempool and newly applied blocks
  * to generate valid block candidates when it is needed. */
class CandidateGenerator(
  minerPk: ProveDlog,
  readersHolderRef: ActorRef,
  viewHolderRef: ActorRef,
  ergoSettings: ErgoSettings
) extends Actor
  with ScorexLogging {

  import org.ergoplatform.mining.CandidateGenerator._

  private val candidateGenInterval =
    ergoSettings.nodeSettings.blockCandidateGenerationInterval

  /** retrieve Readers once on start and then get updated by events */
  override def preStart(): Unit = {
    log.info("CandidateGenerator is starting")
    readersHolderRef ! GetReaders
  }

  /** Send solved block to local blockchain controller */
  private def sendToNodeView(newBlock: ErgoFullBlock): Unit = {
    log.info(
      s"New block ${newBlock.id} w. nonce ${Longs.fromByteArray(newBlock.header.powSolution.n)}"
    )

    // Immediately announce newly mined block to network BEFORE local application.
    // This reduces propagation latency by avoiding the wait for NodeViewHolder
    // to fully validate and apply the block. LocalBlockApplied arrives later
    // and skips broadcast since the block was already announced.
    // TODO: Consider switching to direct actor message for lower latency
    //       instead of event bus publish.
    context.system.eventStream.publish(NewBlockMined(newBlock.header))

    viewHolderRef ! LocallyGeneratedModifier(newBlock.header)
    val sectionsToApply = if (ergoSettings.nodeSettings.stateType == StateType.Digest) {
      newBlock.blockSections
    } else {
      newBlock.mandatoryBlockSections
    }
    sectionsToApply.foreach(viewHolderRef ! LocallyGeneratedModifier(_))
  }

  /**
    * Reaction on invalidation of the block solved by us (e.g. due to a transaction which became
    * invalid after the block candidate was generated): drop the solved block along with cached
    * candidates. Mining will resume on the next external request, which will generate a fresh
    * candidate because the cached one was dropped.
    */
  private def onSolvedBlockFailed(state: CandidateGeneratorState, modId: ModifierId, error: Throwable): Unit = {
    state.solvedBlock.filter(_.toSeq.exists(_.id == modId)).foreach { block =>
      log.warn(
        s"Locally mined block ${block.id} invalidated by the node view holder, resuming mining",
        error
      )
      context.become(
        initialized(state.copy(cachedCandidate = None, cachedPreviousCandidate = None,
          solvedBlock = None, rentScanRefreshPending = None,
          rentScanDeferredCandidate = None))
      )
    }
  }

  override def receive: Receive = {

    // first we need to get Readers to have some initial state to work with
    case Readers(h, s: UtxoStateReader, m, _) =>
      val lastHeaders   = h.lastHeaders(500).headers
      val avgMiningTime = getBlockMiningTimeAvg(lastHeaders.map(_.timestamp))
      val avgTxsCount = getTxsPerBlockCountAvg(
        lastHeaders.flatMap(h.getFullBlock).map(_.transactions.size)
      )
      log.info(
        s"CandidateGenerator initialized, avgMiningTime: ${avgMiningTime.toSeconds}s, avgTxsCount: $avgTxsCount"
      )
      context.become(
        initialized(
          CandidateGeneratorState(
            cachedCandidate = None,
            cachedPreviousCandidate = None,
            solvedBlock = None,
            h,
            s,
            m,
            avgGenTime = 1000.millis,
            lastAppliedBlockTxs = None
          )
        )
      )
      self ! GenerateCandidate(txsToInclude = Seq.empty, reply = false, forced = false)
      context.system.eventStream
        .subscribe(self, classOf[FullBlockApplied])
      context.system.eventStream.subscribe(self, classOf[NodeViewChange])
      context.system.eventStream.subscribe(self, classOf[SemanticallyFailedModification])
      context.system.eventStream.subscribe(self, classOf[SyntacticallyFailedModification])
    case Readers(_, _, _, _) =>
      log.error("Invalid readers state, mining is possible in UTXO mode only")
    case m =>
      // retry until initialized
      context.system.scheduler
        .scheduleOnce(100.millis, self, m)(context.dispatcher, sender())
  }

  private def initialized(state: CandidateGeneratorState): Receive = {
    case ChangedHistory(h: ErgoHistoryReader) =>
      context.become(initialized(state.copy(hr = h, rentScanDeferredCandidate = None)))
    case ChangedState(s: UtxoStateReader) =>
      context.become(initialized(state.copy(sr = s, rentScanDeferredCandidate = None)))
    case ChangedMempool(mp: ErgoMemPoolReader) =>
      if (hasCandidateExpired(
        state.cachedCandidate,
        state.solvedBlock,
        candidateGenInterval
      )) {
        log.debug(s"Regenerating candidate block")
        // with forced = true, state.cachedCandidate will be ignored in GenerateCandidate processing,
        // but state.previousCachedCandidate will be set to cachedCandidate
        context.become(initialized(state.copy(mpr = mp, rentScanDeferredCandidate = None)))
        self ! GenerateCandidate(txsToInclude = Seq.empty, reply = false, forced = true)
      } else {
        context.become(initialized(state.copy(mpr = mp, rentScanDeferredCandidate = None)))
      }
    case _: NodeViewChange =>
    // Just ignore all other NodeView Changes

    /*
     * When new block is applied, either one mined by us or received from peers isn't equal to our candidate's parent,
     * we need to generate new candidate and possibly also discard existing solution if it is also behind
     */
    case applied: FullBlockApplied =>
      val header = applied.header
      log.info(
        s"Preparing new candidate on getting new block at ${header.height}"
      )
      val stateWithAppliedTxs =
        state.copy(lastAppliedBlockTxs = Some(header.id -> applied.txIds.toSet))
      if (needNewCandidate(state.cachedCandidate, header)) {
        if (needNewSolution(state.solvedBlock, header.id))
          context.become(initialized(stateWithAppliedTxs.copy(cachedCandidate = None,
            cachedPreviousCandidate = None, solvedBlock = None, rentScanRefreshPending = None,
            rentScanDeferredCandidate = None)))
        else
          context.become(initialized(stateWithAppliedTxs.copy(cachedCandidate = None,
            cachedPreviousCandidate = None, rentScanRefreshPending = None,
            rentScanDeferredCandidate = None)))
        self ! GenerateCandidate(txsToInclude = Seq.empty, reply = false, forced = false)
      } else {
        context.become(initialized(stateWithAppliedTxs))
      }

    /*
     * If a block solved by us was invalidated by the node view holder, we need to drop it along
     * with cached candidates, as otherwise mining would stall (new solutions are rejected with
     * "Block already solved" and candidate regeneration is paused while solvedBlock is set).
     */
    case SemanticallyFailedModification(_, modId, error) =>
      onSolvedBlockFailed(state, modId, error)

    case SyntacticallyFailedModification(_, modId, error) =>
      onSolvedBlockFailed(state, modId, error)

    case ContinueRentScan(pk, expected) =>
      if (state.rentScanContinuationFor(pk, minerPk).contains(expected)) {
        val next = state.withoutRentScanContinuationFor(pk, minerPk)
        context.become(initialized(next))
        if (next.hasRentScanFor(pk, minerPk) && next.rentScanFor(pk, minerPk) == expected) {
          self ! GenerateRentScan(pk, expected)
        }
      }

    case GenerateRentScan(pk, expected)
        if !state.hasRentScanFor(pk, minerPk) || state.rentScanFor(pk, minerPk) != expected =>
      // A normal candidate request may have advanced the cursor while this tick was queued.
      ()

    case request if request.isInstanceOf[GenerateRentScan] ||
        request.isInstanceOf[GenerateCandidate] =>
      val rentScanOnly = request.isInstanceOf[GenerateRentScan]
      val gen: GenerateCandidate = if (rentScanOnly)
        GenerateCandidate(Seq.empty, reply = false, forced = true,
          optPk = Some(request.asInstanceOf[GenerateRentScan].pk))
        else request.asInstanceOf[GenerateCandidate]
      val txsToInclude = gen.txsToInclude
      val reply = gen.reply
      val forced = gen.forced
      val optPk = gen.optPk
      val senderOpt = if (reply) Some(sender()) else None
      val effectiveMinerPk = optPk.getOrElse(minerPk)
      val selectedRentScan = state.rentScanFor(effectiveMinerPk, minerPk)
      val rentCacheOwnerMatches = cachedFor(state.cachedCandidate, Seq.empty, effectiveMinerPk)
      if (!forced && (!reply || !state.rentScanRefreshPending.contains(effectiveMinerPk)) &&
          cachedFor(state.cachedCandidate, txsToInclude, effectiveMinerPk)) {
        senderOpt.foreach(_ ! StatusReply.success(state.cachedCandidate.get))
      } else if (reply && !forced && state.rentScanRefreshPending.contains(effectiveMinerPk) &&
          cachedFor(state.rentScanDeferredCandidate, txsToInclude, effectiveMinerPk)) {
        // A scan produced this candidate while earlier work was issued. Publish it only
        // on the miner's next request, retaining the earlier work as the previous candidate.
        val candidate = state.rentScanDeferredCandidate.get
        context.become(initialized(state.copy(cachedCandidate = Some(candidate),
          cachedPreviousCandidate = state.cachedCandidate,
          rentScanRefreshPending = None, rentScanDeferredCandidate = None)))
        senderOpt.foreach(_ ! StatusReply.success(candidate))
      } else {
        val start = System.currentTimeMillis()
        val (candidateAttempt, rentProgress) = CandidateGenerator.generateCandidateWithRentScan(
          state.hr,
          state.sr,
          state.mpr,
          effectiveMinerPk,
          txsToInclude,
          state.lastAppliedBlockTxs,
          ergoSettings,
          selectedRentScan,
          rentScanOnly,
          rentCacheOwnerMatches
        )
        def withRentContinuation(next: CandidateGeneratorState): CandidateGeneratorState = {
          val nextScan = next.rentScanFor(effectiveMinerPk, minerPk)
          if (rentProgress.more && next.hasRentScanFor(effectiveMinerPk, minerPk) &&
              !next.rentScanContinuationFor(effectiveMinerPk, minerPk).contains(nextScan)) {
            val delay = if (rentProgress.wrapped) RentScanWrapPause else RentScanTick
            context.system.scheduler.scheduleOnce(delay, self,
              ContinueRentScan(effectiveMinerPk, nextScan))(context.dispatcher)
            next.withRentScanContinuationFor(effectiveMinerPk, minerPk, nextScan)
          } else {
            next
          }
        }
        val rentCacheInvalidated = rentScanOnly && rentCacheOwnerMatches &&
          (rentProgress.state.pendingClaims != selectedRentScan.pendingClaims ||
            rentProgress.state.policy != selectedRentScan.policy ||
            rentProgress.state.tip != selectedRentScan.tip)
        val rentContextChanged = rentScanOnly && rentCacheOwnerMatches &&
          (rentProgress.state.policy != selectedRentScan.policy ||
            rentProgress.state.tip != selectedRentScan.tip)
        // The solution wire format has no work id. Keep issued work in the cache while
        // background rent scans advance; let the next candidate request publish the refresh.
        val preserveIssuedCandidate = rentScanOnly && state.cachedCandidate.nonEmpty &&
          !rentContextChanged
        val markRentRefresh = preserveIssuedCandidate && state.solvedBlock.isEmpty &&
          rentCacheOwnerMatches &&
          (rentCacheInvalidated || candidateAttempt.nonEmpty)
        def withRentProgress(next: CandidateGeneratorState): CandidateGeneratorState = {
          val updated = next.withRentScanFor(effectiveMinerPk, minerPk, rentProgress.state)
          if (markRentRefresh) updated.copy(rentScanRefreshPending = Some(effectiveMinerPk))
          else updated
        }
        candidateAttempt match {
          case Some(Failure(ex)) =>
            log.error(s"Candidate generation failed", ex)
            context.become(initialized(withRentContinuation(
              withRentProgress(state).copy(
                cachedCandidate = if (rentContextChanged) None else state.cachedCandidate,
                cachedPreviousCandidate = if (rentContextChanged) None else state.cachedPreviousCandidate,
                rentScanRefreshPending = if (rentContextChanged) None
                  else if (markRentRefresh) Some(effectiveMinerPk) else state.rentScanRefreshPending,
                rentScanDeferredCandidate = None))))
            senderOpt.foreach(
              _ ! StatusReply.error(s"Candidate generation failed : ${ex.getMessage}")
            )
          case Some(Success((candidate, eliminatedTxs))) =>
            // An autonomous rent scan can stage an unpublished claim against a valid
            // mempool transaction. Do not evict that transaction for speculative work.
            if (!rentScanOnly && eliminatedTxs.ids.nonEmpty) {
              viewHolderRef ! eliminatedTxs
            }
            val generationTook = System.currentTimeMillis() - start
            if (preserveIssuedCandidate) {
              log.debug(s"Rent scan updated in $generationTook ms; keeping issued candidate")
              context.become(initialized(withRentContinuation(withRentProgress(state).copy(
                rentScanDeferredCandidate = if (markRentRefresh) Some(candidate)
                  else state.rentScanDeferredCandidate))))
            } else {
              log.info(s"Generated new candidate in $generationTook ms")
              context.become(initialized(withRentContinuation(withRentProgress(state).copy(
                cachedCandidate = Some(candidate),
                cachedPreviousCandidate = if (rentContextChanged) None else state.cachedCandidate,
                rentScanRefreshPending = None,
                rentScanDeferredCandidate = None,
                avgGenTime = generationTook.millis))))
            }
            senderOpt.foreach(_ ! StatusReply.success(candidate))
          case None =>
            val next = withRentProgress(state).copy(
              cachedCandidate = if (rentContextChanged) None else state.cachedCandidate,
              cachedPreviousCandidate = if (rentContextChanged) None else state.cachedPreviousCandidate,
              rentScanRefreshPending = if (rentContextChanged) None
                else if (markRentRefresh) Some(effectiveMinerPk) else state.rentScanRefreshPending,
              rentScanDeferredCandidate = if (rentScanOnly && !rentCacheInvalidated &&
                  !rentContextChanged) state.rentScanDeferredCandidate else None)
            context.become(initialized(if (reply) next else withRentContinuation(next)))
            if (!rentScanOnly) {
              log.warn(
                "Can not generate block candidate: either mempool is empty or chain is not synced (maybe last block not fully applied yet"
              )
            }
            senderOpt.foreach { s =>
              context.system.scheduler.scheduleOnce(state.avgGenTime, self, gen)(
                context.system.dispatcher,
                s
              )
            }
        }
      }

    case preSolution: AutolykosSolution
        if state.solvedBlock.isEmpty && state.cachedCandidate.nonEmpty =>
      // Inject node pk if it is not externally set (in Autolykos 2)
      val solution =
        if (CryptoFacade.isInfinityPoint(preSolution.pk)) {
          AutolykosSolution(minerPk.value, preSolution.w, preSolution.n, preSolution.d)
        } else {
          preSolution
        }
      val result: StatusReply[Unit] = {
        val newBlock = state.cachedCandidate
          .map(candidate => completeBlock(candidate.candidateBlock, solution))
          .filter(block => ergoSettings.chainSettings.powScheme.validate(block.header).isSuccess)
          .getOrElse {
            log.info(s"Using previous candidate as a solution: " + state.cachedPreviousCandidate)
            completeBlock(state.cachedPreviousCandidate.get.candidateBlock, solution)
          }
        log.info(s"New block mined, header: ${newBlock.header}")
        ergoSettings.chainSettings.powScheme.validate(newBlock.header) match {
          case Success(_) =>
            sendToNodeView(newBlock)
            context.become(initialized(state.copy(solvedBlock = Some(newBlock),
              rentScanRefreshPending = None, rentScanDeferredCandidate = None)))
            StatusReply.success(())
          case Failure(exception) =>
            log.warn(s"Removing candidates due to invalid block", exception)
            context.become(initialized(state.copy(cachedCandidate = None,
              cachedPreviousCandidate = None, rentScanRefreshPending = None,
              rentScanDeferredCandidate = None)))
            StatusReply.error(
              new Exception(s"Invalid block mined: ${exception.getMessage}", exception)
            )
        }
      }
      log.info(s"Processed solution $solution with the result $result")
      sender() ! result

    case _: AutolykosSolution =>
      sender() ! StatusReply.error(
        s"Block already solved : ${state.solvedBlock.map(_.id)}"
      )

  }

}

object CandidateGenerator extends ScorexLogging {

  private final class NoCandidateTransactions extends IllegalArgumentException(
    "Proofs for 0 txs cannot be generated")

  /**
    * Holder for both candidate block and data for external miners derived from it
    * (to avoid possibly costly recalculation)
    *
    * @param candidateBlock  - block candidate
    * @param externalVersion - message for external miner
    * @param txsToInclude    - transactions which were prioritized for inclusion in the block candidate
    */
  case class Candidate(
    candidateBlock: CandidateBlock,
    externalVersion: WorkMessage,
    txsToInclude: Seq[ErgoTransaction]
  )

  case class GenerateCandidate(
    txsToInclude: Seq[ErgoTransaction],
    reply: Boolean,
    forced: Boolean,
    optPk: Option[ProveDlog] = None
  )

  private[mining] case class ContinueRentScan(pk: ProveDlog, expected: RentScanState)
  private case class GenerateRentScan(pk: ProveDlog, expected: RentScanState)

  private[mining] case class RentScanPolicy(parameters: Map[Byte, Int],
                                          blockVersion: Byte,
                                          minerPk: ProveDlog,
                                          reemissionToken: Option[ModifierId],
                                          tokenWhitelist: Set[ModifierId])

  private[mining] case class RentScanState(after: Option[Vector[Byte]] = None,
                                         ceiling: Option[Int] = None,
                                         tip: Option[(Int, ModifierId)] = None,
                                         policy: Option[RentScanPolicy] = None,
                                         pendingClaims: Seq[Seq[ModifierId]] = Seq.empty)

  private[mining] case class RentScanProgress(state: RentScanState,
                                            more: Boolean = false,
                                            wrapped: Boolean = false)

  private val RentRawKeysPerPage = StorageRentClaimBuilder.MaxClaims
  private val RentPagesPerAttempt = 2
  private val MaxPendingRentClaims = 4
  private val MaxAlternateRentScans = 4
  private val RentScanTick = 500.millis
  private val RentScanWrapPause = 5.seconds

  /** The best header branch may differ from the selected full-block branch. */
  private[mining] def continuesSelectedFullChain(history: ErgoHistoryReader,
                                                 previous: Option[(Int, ModifierId)],
                                                 current: Option[(Int, ModifierId)]): Boolean =
    previous.exists { case (height, id) =>
      current.exists { case (currentHeight, currentId) =>
        currentHeight >= height &&
          (currentId == id || history.isInSelectedFullChain(id))
      }
    }

  /** Local state of candidate generator to avoid mutable vars */
  case class CandidateGeneratorState(
    cachedCandidate: Option[Candidate],
    cachedPreviousCandidate: Option[Candidate],
    solvedBlock: Option[ErgoFullBlock],
    hr: ErgoHistoryReader,
    sr: UtxoStateReader,
    mpr: ErgoMemPoolReader,
    avgGenTime: FiniteDuration, // approximation of average block generation time for more efficient retries
    lastAppliedBlockTxs: Option[(ModifierId, Set[ModifierId])], // header id and tx ids of the last applied block
    rentScan: RentScanState = RentScanState(),
    rentScanContinuationPending: Option[RentScanState] = None,
    alternateRentScans: Vector[(ProveDlog, RentScanState)] = Vector.empty,
    alternateRentScanContinuationPending: Vector[(ProveDlog, RentScanState)] = Vector.empty,
    rentScanRefreshPending: Option[ProveDlog] = None,
    rentScanDeferredCandidate: Option[Candidate] = None
  ) {
    private[mining] def hasRentScanFor(pk: ProveDlog, defaultPk: ProveDlog): Boolean =
      pk == defaultPk || alternateRentScans.exists(_._1 == pk)

    private[mining] def rentScanFor(pk: ProveDlog, defaultPk: ProveDlog): RentScanState =
      if (pk == defaultPk) rentScan
      else alternateRentScans.find(_._1 == pk).map(_._2).getOrElse(RentScanState())

    private[mining] def rentScanContinuationFor(pk: ProveDlog,
                                                defaultPk: ProveDlog): Option[RentScanState] =
      if (pk == defaultPk) rentScanContinuationPending
      else alternateRentScanContinuationPending.find(_._1 == pk).map(_._2)

    private[mining] def withRentScanContinuationFor(pk: ProveDlog,
                                                    defaultPk: ProveDlog,
                                                    scan: RentScanState): CandidateGeneratorState =
      if (pk == defaultPk) copy(rentScanContinuationPending = Some(scan))
      else copy(alternateRentScanContinuationPending =
        (alternateRentScanContinuationPending.filterNot(_._1 == pk) :+ (pk -> scan))
          .takeRight(MaxAlternateRentScans))

    private[mining] def withoutRentScanContinuationFor(pk: ProveDlog,
                                                       defaultPk: ProveDlog): CandidateGeneratorState =
      if (pk == defaultPk) copy(rentScanContinuationPending = None)
      else copy(alternateRentScanContinuationPending =
        alternateRentScanContinuationPending.filterNot(_._1 == pk))

    private[mining] def withRentScanFor(pk: ProveDlog,
                        defaultPk: ProveDlog,
                        scan: RentScanState): CandidateGeneratorState =
      if (pk == defaultPk) copy(rentScan = scan)
      else {
        val scans = (alternateRentScans.filterNot(_._1 == pk) :+ (pk -> scan))
          .takeRight(MaxAlternateRentScans)
        copy(alternateRentScans = scans,
          alternateRentScanContinuationPending = alternateRentScanContinuationPending
            .filter { case (key, _) => scans.exists(_._1 == key) })
      }
  }

  def apply(
    minerPk: ProveDlog,
    readersHolderRef: ActorRef,
    viewHolderRef: ActorRef,
    ergoSettings: ErgoSettings
  )(implicit context: ActorRefFactory): ActorRef =
    context.actorOf(
      Props(
        new CandidateGenerator(
          minerPk,
          readersHolderRef,
          viewHolderRef,
          ergoSettings
        )
      ).withDispatcher("critical-dispatcher"),
      s"CandidateGenerator-${Random.alphanumeric.take(5).mkString}"
    )

  /**
   * Checks that current candidate block is cached with given `txs` and `minerPk`.
   *
   * Note: candidate cache is a single slot keyed by `minerPk`. If multiple miner public keys
   * are used concurrently (e.g. node’s own miner and external `/mining/candidateWithTxsAndPk`
   * callers), each different `minerPk` will evict the previous cached candidate and trigger
   * full candidate generation (mempool packing + state proofs). This endpoint assumes a single
   * active miner public key at a time for optimal performance.
   */
  def cachedFor(
    candidateOpt: Option[Candidate],
    txs: Seq[ErgoTransaction],
    minerPk: ProveDlog
  ): Boolean = {
    candidateOpt.isDefined && candidateOpt.exists { c =>
      c.externalVersion.pk == minerPk &&
        (txs.isEmpty || (txs.size == c.txsToInclude.size && txs.forall(
          c.txsToInclude.contains
        )))
    }
  }

  /** we need new candidate if given block is not parent of our cached block */
  def needNewCandidate(
    cache: Option[Candidate],
    bestFullBlockHeader: Header
  ): Boolean = {
    val parentHeaderIdOpt = cache.map(_.candidateBlock).flatMap(_.parentOpt).map(_.id)
    !parentHeaderIdOpt.contains(bestFullBlockHeader.id)
  }

  /** Solution is valid only if bestFullBlock on the chain is its parent */
  def needNewSolution(
    solvedBlock: Option[ErgoFullBlock],
    bestFullBlockId: ModifierId
  ): Boolean = {
    solvedBlock.nonEmpty && !solvedBlock.map(_.parentId).contains(bestFullBlockId)
  }

  /** Regenerate candidate to let new transactions in, miners are polling for candidate in ~ 100ms
    * interval so they switch to it.
    * If blockCandidateGenerationInterval elapsed since last block generation,
    * then new tx in mempool is a reasonable trigger of candidate regeneration
    */
  def hasCandidateExpired(
    cachedCandidate: Option[Candidate],
    solvedBlock: Option[ErgoFullBlock],
    candidateGenInterval: FiniteDuration
   ): Boolean = {
    def candidateAge(c: Candidate): FiniteDuration =
      (System.currentTimeMillis() - c.candidateBlock.timestamp).millis
    // non-empty solved block means we wait for newly mined block to be applied
    if (solvedBlock.isDefined) {
      false
    } else {
      cachedCandidate match {
        // if current candidate is older than candidateGenInterval
        case Some(c) if candidateGenInterval.compare(candidateAge(c)) <= 0 =>
          log.info(s"Regenerating block candidate")
          true
        case _ =>
          false
      }
    }
  }

  /** Calculate average mining time from latest block header timestamps */
  def getBlockMiningTimeAvg(
    timestamps: IndexedSeq[Header.Timestamp]
  ): FiniteDuration = {
    val miningTimes =
      timestamps.sorted
        .sliding(2, 1)
        .map { case IndexedSeq(prev, next) => next - prev }
        .toVector
    Math.round(miningTimes.sum / miningTimes.length.toDouble).millis
  }

  /** Get average count of transactions per block */
  def getTxsPerBlockCountAvg(txsPerBlock: IndexedSeq[Int]): Long =
    Math.round(txsPerBlock.sum / txsPerBlock.length.toDouble)

  /** Helper which is checking that inputs of the transaction are not spent */
  private def inputsNotSpent(tx: ErgoTransaction, s: UtxoStateReader): Boolean =
    tx.inputs.forall(inp => s.boxById(inp.boxId).isDefined)

  /**
    * Whether `tx` is a storage-rent claim: spends inputs with empty proofs, each pointing
    * at its own output via the var #127 (StorageIndexVarId) context extension.
    */
  def isStorageRentClaim(tx: ErgoTransaction): Boolean =
    tx.inputs.exists { in =>
      in.spendingProof.proof.isEmpty &&
        in.spendingProof.extension.values.contains(Constants.StorageIndexVarId)
    }

  /**
    * Ids of boxes spent by the storage-rent claim transactions among `txs`, in order.
    */
  def rentClaimSpentBoxIds(txs: Seq[ErgoTransaction]): Seq[ModifierId] =
    txs.filter(isStorageRentClaim).flatMap(tx => tx.inputs.map(in => bytesToId(in.boxId)))

  /**
    * Checks that the best full block in the history corresponds to the state.
    * Evaluated via live history storage reads, so re-checking it after candidate assembly
    * detects a block applied concurrently with the assembly.
    */
  def isChainSynced(
    bestFullBlockIdOpt: Option[ModifierId],
    stateContext: ErgoStateContext
  ): Boolean =
    bestFullBlockIdOpt == stateContext.lastHeaderOpt.map(_.id)

  /**
    * Filters out from `poolTxs` transactions included into the last applied block
    * (`lastAppliedBlockTxs`), if the block is still the best full block (`bestFullBlockIdOpt`).
    * Such transactions are removed from the mempool by the node view holder itself on block
    * application, so there is no need to validate them during candidate assembly (which logs
    * misleading double-spending messages) nor to eliminate them via EliminateTransactions.
    */
  def excludeAppliedTxs(
    poolTxs: Seq[UnconfirmedTransaction],
    lastAppliedBlockTxs: Option[(ModifierId, Set[ModifierId])],
    bestFullBlockIdOpt: Option[ModifierId]
  ): Seq[UnconfirmedTransaction] = {
    lastAppliedBlockTxs match {
      case Some((appliedHeaderId, appliedTxIds))
          if appliedTxIds.nonEmpty && bestFullBlockIdOpt.contains(appliedHeaderId) =>
        poolTxs.filterNot(tx => appliedTxIds.contains(tx.id))
      case _ =>
        poolTxs
    }
  }

  /**
    * @return None if chain is not synced or Some of attempt to create candidate
    */
  def generateCandidate(
    h: ErgoHistoryReader,
    s: UtxoStateReader,
    m: ErgoMemPoolReader,
    pk: ProveDlog,
    txsToInclude: Seq[ErgoTransaction],
    lastAppliedBlockTxs: Option[(ModifierId, Set[ModifierId])],
    ergoSettings: ErgoSettings
  ): Option[Try[(Candidate, EliminateTransactions)]] =
    generateCandidateWithRentScan(h, s, m, pk, txsToInclude,
      lastAppliedBlockTxs, ergoSettings, RentScanState())._1

  private[mining] def generateCandidateWithRentScan(
    h: ErgoHistoryReader,
    s: UtxoStateReader,
    m: ErgoMemPoolReader,
    pk: ProveDlog,
    txsToInclude: Seq[ErgoTransaction],
    lastAppliedBlockTxs: Option[(ModifierId, Set[ModifierId])],
    ergoSettings: ErgoSettings,
    rentScan: RentScanState,
    rentScanOnly: Boolean = false,
    rentCacheOwnerMatches: Boolean = true
  ): (Option[Try[(Candidate, EliminateTransactions)]], RentScanProgress) = {
    var rentProgress = RentScanProgress(rentScan)
    def unchanged: RentScanProgress = RentScanProgress(rentScan)
    // mandatory transactions to include into next block taken from the previous candidate
    val stateWithMandatoryTxs = s.withTransactions(txsToInclude)
    lazy val unspentTxsToInclude = txsToInclude.filter { tx =>
      inputsNotSpent(tx, stateWithMandatoryTxs)
    }

    val stateContext = s.stateContext

    //only transactions valid from against the current utxo state we take from the mem pool,
    //skipping transactions already included into the last applied block
    lazy val poolTransactions =
      excludeAppliedTxs(m.getAllPrioritized, lastAppliedBlockTxs, h.bestFullBlockOpt.map(_.id))

    lazy val emissionTxOpt =
      CandidateGenerator.collectEmission(s, pk, stateContext)

    def chainSynced =
      isChainSynced(h.bestFullBlockOpt.map(_.id), stateContext)

    def hasAnyMemPoolOrMinerTx =
      poolTransactions.nonEmpty || unspentTxsToInclude.nonEmpty || emissionTxOpt.nonEmpty

    // A bounded probe can skip assembly only when the rent prefix is exhausted.
    // A foreign key may precede the first rent row, so its presence keeps the
    // resumable scan alive even if this first raw page has no rent row.
    def hasIndexedRentBox = {
      val threshold = stateContext.currentHeight + 1 - Constants.StoragePeriod
      ergoSettings.nodeSettings.storageRentCollection && threshold > 0 &&
        Try {
          val page = h.storageRentBoxesPage(threshold, 1, None)
          page.rows.nonEmpty || page.hasMore
        }.getOrElse(true)
    }

    if (!hasAnyMemPoolOrMinerTx && !hasIndexedRentBox) {
      log.info("Avoiding generation of a block without any transactions")
      (None, unchanged)
    } else if (!chainSynced) {
      log.info(
        "Chain not synced probably due to racing condition when last block is not fully applied yet"
      )
      (None, unchanged)
    } else {
      val desiredUpdate = if (stateContext.blockVersion == 3) {
        ergoSettings.votingTargets.desiredUpdate.copy(statusUpdates =
          // 1007 is needed to switch off primitive type validation to add Unsigned Big Int support
          // 1008 is needed to switch off non-primitive type validation to add Option & Header types support
          // 1011 is needed to add new methods
          Seq(1011.toShort -> ReplacedRule(1016), 1007.toShort -> ReplacedRule(1017), 1008.toShort -> ReplacedRule(1018)))
      } else {
        ergoSettings.votingTargets.desiredUpdate
      }
      val candidateAttempt = createCandidate(
        pk,
        h,
        desiredUpdate,
        s,
        poolTransactions,
        emissionTxOpt,
        unspentTxsToInclude,
        ergoSettings,
        rentScan,
        progress => rentProgress = progress,
        rentScanOnly,
        rentCacheOwnerMatches
      )
      if (!chainSynced) {
        log.debug(
          "Discarding block candidate as a new block was applied during its assembly, " +
          "a new candidate will be generated on FullBlockApplied"
        )
        (None, unchanged)
      } else {
        candidateAttempt match {
          case Failure(_: NoCandidateTransactions) =>
            if (!rentScanOnly)
              log.info("Avoiding generation of a block without any transactions")
            (None, rentProgress)
          case Failure(_) => (Some(candidateAttempt), rentProgress)
          case _ => (Some(candidateAttempt), rentProgress)
        }
      }
    }
  }

  /**
    * Private method which suggests to vote for soft-fork (or not)
    *
    * @param ergoSettings - constant settings
    * @param currentParams - network parameters after last block mined
    * @param header - last mined header
    * @return `true` if the node should vote for soft-fork
    */
  private def forkOrdered(ergoSettings: ErgoSettings, currentParams: Parameters, header: Header): Boolean = {
    val nextHeight = header.height + 1

    val protocolVersion = ergoSettings.chainSettings.protocolVersion

    // if protocol version is 2 (node version 4.x, we still can vote for 5.0 soft-fork)
    val betterVersion = if (ergoSettings.networkType.isMainNet && protocolVersion == 2) {
      true
    } else {
      protocolVersion > header.version
    }

    val votingSettings = ergoSettings.chainSettings.voting
    val votingFinishHeight: Option[Height] = currentParams.softForkStartingHeight
      .map(_ + votingSettings.votingLength * votingSettings.softForkEpochs)
    val forkVotingAllowed = votingFinishHeight.forall(fh => nextHeight < fh)

    val nextHeightCondition = if (ergoSettings.networkType.isMainNet) {
      nextHeight >= 1561601 // 6.0 voting starting height, first block of epoch #1525
    } else if(ergoSettings.networkType.isTestNet) {
      nextHeight >= 1548800 // testnet voting start height
    } else {
      nextHeight >= 8 // devnet voting start height
    }

    // we automatically vote for 5.0 soft-fork in the mainnet if 120 = 0 vote not provided in settings
    val forkOrdered = if (ergoSettings.networkType.isMainNet && protocolVersion == 2) {
      ergoSettings.votingTargets.softForkOption.getOrElse(1) == 1
    } else {
      ergoSettings.votingTargets.softForkOption.getOrElse(0) == 1
    }

    //todo: remove after 6.0 soft-fork activation
    log.debug(s"betterVersion: $betterVersion, forkVotingAllowed: $forkVotingAllowed, " +
              s"forkOrdered: $forkOrdered, nextHeightCondition: $nextHeightCondition")

    betterVersion &&
      forkVotingAllowed &&
      forkOrdered &&
      nextHeightCondition
  }

  /**
    * Assemble correct block candidate based on
    *
    * @param minerPk                 - public key of the miner
    * @param history                 - blockchain reader (to extract parent)
    * @param proposedUpdate          - votes for parameters update or/and soft-fork
    * @param state                   - UTXO set reader
    * @param poolTxs                 - memory pool transactions
    * @param emissionTxOpt           - optional emission transaction
    * @param prioritizedTransactions - transactions which are going into the block in the first place
    *                                (before transactions from the pool). No guarantee of inclusion in general case.
    * @return - candidate or an error
    */
  def createCandidate(
                       minerPk: ProveDlog,
                       history: ErgoHistoryReader,
                       proposedUpdate: ErgoValidationSettingsUpdate,
                       state: UtxoStateReader,
                       poolTxs: Seq[UnconfirmedTransaction],
                       emissionTxOpt: Option[ErgoTransaction],
                       prioritizedTransactions: Seq[ErgoTransaction],
                        ergoSettings: ErgoSettings,
                        rentScan: RentScanState,
                        onRentScan: RentScanProgress => Unit,
                        rentScanOnly: Boolean,
                        rentCacheOwnerMatches: Boolean
  ): Try[(Candidate, EliminateTransactions)] =
    Try {
      val popowAlgos = new NipopowAlgos(ergoSettings.chainSettings)
      // Extract best header and extension of a best block user their data for assembling a new block
      val bestHeaderOpt: Option[Header] = history.bestFullBlockOpt.map(_.header)
      val bestExtensionOpt: Option[Extension] = bestHeaderOpt
        .flatMap(h => history.typedModifierById[Extension](h.extensionId))

      // Make progress in time since last block.
      // If no progress is made, then, by consensus rules, the block will be rejected.
      val timestamp =
        Math.max(System.currentTimeMillis(), bestHeaderOpt.map(_.timestamp + 1).getOrElse(0L))

      val stateContext = state.stateContext

      // Calculate required difficulty for the new block
      val nBits: Long = bestHeaderOpt
        .map(parent => history.requiredDifficultyAfter(parent))
        .map(d => DifficultySerializer.encodeCompactBits(d))
        .getOrElse(ergoSettings.chainSettings.initialNBits)

      // Obtain NiPoPoW interlinks vector to pack it into the extension section
      val updInterlinks       = popowAlgos.updateInterlinks(bestHeaderOpt, bestExtensionOpt)
      val interlinksExtension = popowAlgos.interlinksToExtension(updInterlinks)
      val votingSettings      = ergoSettings.chainSettings.voting
      val (extensionCandidate, votes: Array[Byte], version: Byte) = bestHeaderOpt
        .map { header =>
          val newHeight     = header.height + 1
          val currentParams = stateContext.currentParameters
          val voteForSoftFork = forkOrdered(ergoSettings, currentParams, header)

          if (newHeight % votingSettings.votingLength == 0 && newHeight > 0) {
            // new voting epoch
            val (newParams, activatedUpdate) = currentParams.update(
              newHeight,
              voteForSoftFork,
              stateContext.votingData.epochVotes,
              proposedUpdate,
              votingSettings
            )
            val newValidationSettings = stateContext.validationSettings.updated(activatedUpdate)
            (
              newParams.toExtensionCandidate ++ interlinksExtension ++ newValidationSettings.toExtensionCandidate,
              newParams.suggestVotes(ergoSettings.votingTargets.targets, voteForSoftFork),
              newParams.blockVersion
            )
          } else {
            val votes = currentParams.vote(
              ergoSettings.votingTargets.targets,
              stateContext.votingData.epochVotes,
              voteForSoftFork
            )
            (
              interlinksExtension,
              votes,
              currentParams.blockVersion
            )
          }
        }
        .getOrElse(
          (interlinksExtension, Array(0: Byte, 0: Byte, 0: Byte), Header.InitialVersion)
        )

      val upcomingContext = state.stateContext.upcoming(
        minerPk.value,
        timestamp,
        nBits,
        votes,
        proposedUpdate,
        version
      )

      val emissionTxs = emissionTxOpt.toSeq

      // storage-rent self-claim: sweep rent-eligible boxes directly into the candidate,
      // bypassing the mempool (goes into the block right after the prioritized transactions)
      var rentProgress = RentScanProgress(rentScan)
      var scanFoundNewClaim = false
      val rentClaimTxs: Seq[ErgoTransaction] =
        if (ergoSettings.nodeSettings.storageRentCollection) {
          val upcomingHeight = upcomingContext.currentHeight
          val threshold = upcomingHeight - Constants.StoragePeriod
          if (threshold > 0) {
            val params = upcomingContext.currentParameters
            val reemissionTokenId = Option(ergoSettings.chainSettings.reemission.reemissionTokenId)
              .filter(_.nonEmpty)
            val tokenWhitelist = ergoSettings.nodeSettings.storageRentTokenWhitelist
              .toSet
            val policy = RentScanPolicy(params.parametersTable, version, minerPk,
              reemissionTokenId, tokenWhitelist)
            val tip = bestHeaderOpt.map(h => h.height -> h.id)
            val sameSelectedChain = continuesSelectedFullChain(history, rentScan.tip, tip)
            val continueSweep = rentScan.policy.contains(policy) && sameSelectedChain
            val ceiling = if (continueSweep) rentScan.ceiling.getOrElse(threshold)
              else threshold
            var after = if (continueSweep)
              rentScan.after else None
            // Candidate inclusion is speculative. Rebuild each bounded pending claim from
            // current UTXO boxes, independently of the forward-only index cursor.
            val pendingClaims = rentScan.pendingClaims.flatMap { ids =>
              val boxes = ids.flatMap(id => state.boxById(ADKey @@ idToBytes(id)))
              StorageRentClaimBuilder.buildClaim(boxes, upcomingHeight, params,
                minerPk, reemissionTokenId, tokenWhitelist).map { tx =>
                tx.inputs.map(in => bytesToId(in.boxId)).toSeq -> tx
              }
            }.take(MaxPendingRentClaims)
            val pendingIds = pendingClaims.map(_._1)
            val pendingBoxIds = pendingIds.flatten.toSet
            var more = true
            var pages = 0
            var claim: Option[ErgoTransaction] = None
            // A bounded scan page may contain only boxes that current parameters or miner
            // policy cannot claim. Move past them without modifying the chain-derived index.
            while (pages < RentPagesPerAttempt && more && claim.isEmpty) {
              val page = history.storageRentBoxesPage(ceiling, RentRawKeysPerPage, after)
              more = page.hasMore
              after = page.after
              val resolved = page.rows.toSeq.filterNot(entry =>
                pendingBoxIds.contains(entry.boxId)).flatMap(entry =>
                state.boxById(ADKey @@ idToBytes(entry.boxId)))
              claim = StorageRentClaimBuilder.buildClaim(resolved, upcomingHeight, params,
                minerPk, reemissionTokenId, tokenWhitelist)
              scanFoundNewClaim = claim.nonEmpty
              pages += 1
            }
            val wrapped = !more
            val nextScan = RentScanState(after, if (wrapped) None else Some(ceiling),
              tip, Some(policy), pendingIds)
            rentProgress = RentScanProgress(nextScan, more = true, wrapped = wrapped)
            onRentScan(rentProgress)
            // Rotate a full speculative window so later eligible rows still enter
            // candidates; evicted rows remain indexed for the next sweep. Give the
            // newly discovered claim the first rent slot: block packing stops at
            // the first cost or size overflow.
            val retained = if (scanFoundNewClaim)
              pendingClaims.take(MaxPendingRentClaims - 1) else pendingClaims
            claim.toSeq ++ retained.map(_._2)
          } else {
            rentProgress = RentScanProgress(RentScanState())
            onRentScan(rentProgress)
            Seq.empty
          }
        } else {
          rentProgress = RentScanProgress(RentScanState())
          onRentScan(rentProgress)
          Seq.empty
        }

      if (rentClaimTxs.nonEmpty) {
        log.debug(s"Storage-rent claim transactions injected into the candidate: ${rentClaimTxs.map(_.id)}")
      }

      // A continuation with no new claim only advances the cursor. The cached candidate
      // already contains the revalidated pending claims, so no AVL proof is needed.
      if (rentScanOnly && !scanFoundNewClaim &&
          rentProgress.state.pendingClaims == rentScan.pendingClaims &&
          rentProgress.state.policy == rentScan.policy &&
          rentProgress.state.tip == rentScan.tip &&
          (rentProgress.state.pendingClaims.isEmpty || rentCacheOwnerMatches)) {
        throw new NoCandidateTransactions
      }

      val candidateTxs = emissionTxs ++ prioritizedTransactions ++ rentClaimTxs ++ poolTxs.map(_.transaction)
      if (candidateTxs.isEmpty) {
        throw new NoCandidateTransactions
      }

      // todo: remove in 5.0
      // we allow for some gap, to avoid possible problems when different interpreter version can estimate cost
      // differently due to bugs in AOT costing
      val safeGap = if (state.stateContext.currentParameters.maxBlockCost < 1000000) {
        0
      } else if (state.stateContext.currentParameters.maxBlockCost < 5000000) {
        150000
      } else {
        500000
      }

      // Candidate-local conflicts and transient validation failures cannot change the
      // persistent unspent index. The extra indexer owns deletion on selected-chain spends.
      def collectPoolTxs: (Seq[ErgoTransaction], Seq[ModifierId]) = {
        collectTxs(
          minerPk,
          state.stateContext.currentParameters.maxBlockCost - safeGap,
          state.stateContext.currentParameters.maxBlockSize,
          state,
          upcomingContext,
          candidateTxs,
          rentClaimTxs.map(_.id).toSet
        )
      }

      val (txs, toEliminate) = collectPoolTxs

      val eliminateTransactions = EliminateTransactions(toEliminate)

      if (txs.isEmpty) {
        throw new IllegalArgumentException(
          s"Proofs for 0 txs cannot be generated : emissionTxs: ${emissionTxs.size}, priorityTxs: ${prioritizedTransactions.size}, poolTxs: ${poolTxs.size}"
        )
      }

      def deriveWorkMessage(block: CandidateBlock) = {
        ergoSettings.chainSettings.powScheme.deriveExternalCandidate(
          block,
          minerPk,
          prioritizedTransactions.map(_.id)
        )
      }

      def mkCandidate(blockTxs: Seq[ErgoTransaction],
                      adProof: SerializedAdProof,
                      adDigest: ADDigest,
                      eliminate: EliminateTransactions): (Candidate, EliminateTransactions) = {
        val includedClaims = rentClaimTxs.filter(claim => blockTxs.exists(_.id == claim.id))
          .map(claim => claim.inputs.map(in => bytesToId(in.boxId)).toSeq)
        onRentScan(rentProgress.copy(
          state = rentProgress.state.copy(pendingClaims = includedClaims)))
        val candidate = CandidateBlock(
          bestHeaderOpt, version, nBits, adDigest,
          adProof, blockTxs, timestamp, extensionCandidate, votes
        )
        val ext = deriveWorkMessage(candidate)
        log.info(
          s"Got candidate block at height ${ErgoHistoryUtils.heightOf(candidate.parentOpt) + 1}" +
          s" with ${candidate.transactions.size} transactions, msg ${Base16.encode(ext.msg)}"
        )
        Candidate(candidate, ext, prioritizedTransactions) -> eliminate
      }

      state.proofsForTransactions(txs) match {
        case Success((adProof, adDigest)) =>
          Success(mkCandidate(txs, adProof, adDigest, eliminateTransactions))
        case Failure(t: Throwable) =>
          // A likely reason of the failure is a state update (new block applied) between
          // collectTxs and proofsForTransactions. Re-collect transactions against the current
          // state and retry once before falling back to an emission-only candidate.
          val (retryTxs, retryToEliminate) = collectPoolTxs
          // The first pass may have rejected transactions against a transient state.
          // Keep only the classifications from the latest collection attempt.
          log.error("Retrying candidate generation after failed proofs")
          val retryEliminate = EliminateTransactions(retryToEliminate)
          state.proofsForTransactions(retryTxs) match {
            case Success((adProof, adDigest)) =>
              log.warn(
                s"Proof generation failed once (${t.getMessage}), " +
                s"recovered on retry with ${retryTxs.size} transactions"
              )
              Success(mkCandidate(retryTxs, adProof, adDigest, retryEliminate))
            case Failure(ex: Throwable) =>
              // We can not produce a block for some reason, so print out an error
              // and collect only emission transaction if it exists.
              // We consider that emission transaction is always valid.
              emissionTxOpt match {
                case Some(emissionTx) =>
                  log.error("Failed to produce proofs for transactions, but emission box is found: ", ex)
                  state.proofsForTransactions(Seq(emissionTx)).map {
                    case (adProof, adDigest) =>
                      // Both collections produced failed proofs; their rejections may be stale.
                      mkCandidate(Seq(emissionTx), adProof, adDigest, EliminateTransactions(Seq.empty))
                  }
                case None =>
                  log.error("Failed to produce proofs for transactions and no emission box available: ", ex)
                  Failure(ex)
              }
          }
      }
    }.flatten

  /**
    * Transaction and its cost.
    */
  type CostedTransaction = (ErgoTransaction, Int)

  //TODO move ErgoMiner to mining package and make `collectTxs` and `fixTxsConflicts` private[mining]

  def collectEmission(
    state: UtxoStateReader,
    minerPk: ProveDlog,
    stateContext: ErgoStateContext
  ): Option[ErgoTransaction] = {
    collectRewards(
      state.emissionBoxOpt,
      state.stateContext.currentHeight,
      Seq.empty,
      minerPk,
      stateContext,
      Colls.emptyColl
    ).headOption
  }

  def collectFees(
    currentHeight: Int,
    txs: Seq[ErgoTransaction],
    minerPk: ProveDlog,
    stateContext: ErgoStateContext
  ): Option[ErgoTransaction] = {
    collectRewards(None, currentHeight, txs, minerPk, stateContext, Colls.emptyColl).headOption
  }

  /**
    * Generate from 0 to 2 transaction that collecting rewards from fee boxes in block transactions `txs` and
    * emission box `emissionBoxOpt`
    */
  def collectRewards(
    emissionBoxOpt: Option[ErgoBox],
    currentHeight: Int,
    txs: Seq[ErgoTransaction],
    minerPk: ProveDlog,
    stateContext: ErgoStateContext,
    assets: Coll[(TokenId, Long)] = Colls.emptyColl
  ): Seq[ErgoTransaction] = {
    val chainSettings = stateContext.chainSettings
    val propositionBytes = chainSettings.monetary.feePropositionBytes
    val emission = chainSettings.emissionRules

    // forming transaction collecting emission
    val reemissionSettings = chainSettings.reemission
    val reemissionRules = reemissionSettings.reemissionRules

    val eip27ActivationHeight = reemissionSettings.activationHeight
    val reemissionTokenId = Digest32Coll @@ reemissionSettings.reemissionTokenIdBytes

    val nextHeight = currentHeight + 1
    val minerProp =
      ErgoTreePredef.rewardOutputScript(emission.settings.minerRewardDelay, minerPk)

    val emissionTxOpt: Option[ErgoTransaction] = emissionBoxOpt.map { emissionBox =>
      val prop           = emissionBox.ergoTree
      val emissionAmount = emission.minersRewardAtHeight(nextHeight)

      // how many nanoERG should be re-emitted
      lazy val reemissionAmount = reemissionRules.reemissionForHeight(nextHeight, emission)

      val emissionBoxAssets: Coll[(TokenId, Long)] = if (nextHeight == eip27ActivationHeight) {
        // we inject emission box NFT and reemission tokens on activation height
        // see "Activation Details" section of EIP-27
        val injTokens = reemissionSettings.injectionBox.additionalTokens

        //swap tokens if emission NFT is going after reemission
        if (injTokens.apply(1)._2 == 1) {
          Colls.fromItems(injTokens.apply(1), injTokens.apply(0))
        } else {
          injTokens
        }
      } else {
        emissionBox.additionalTokens
      }

      val updEmissionAssets = if (nextHeight >= eip27ActivationHeight) {
        // deduct reemission from emission box
        val reemissionTokens = emissionBoxAssets.apply(1)._2
        val updAmount = reemissionTokens - reemissionAmount
        emissionBoxAssets.updated(1, reemissionTokenId -> updAmount)
      } else {
        emissionBoxAssets
      }

      val newEmissionBox: ErgoBoxCandidate =
        new ErgoBoxCandidate(emissionBox.value - emissionAmount, prop, nextHeight, updEmissionAssets)
      val inputs = if (nextHeight == eip27ActivationHeight) {
        // injection - second input is injection box
        IndexedSeq(
          new Input(emissionBox.id, ProverResult.empty),
          new Input(reemissionSettings.injectionBox.id, ProverResult.empty)
        )
      } else {
        IndexedSeq(new Input(emissionBox.id, ProverResult.empty))
      }

      val minerAmt = if (nextHeight == eip27ActivationHeight) {
        // injection - injection box value going to miner
        emissionAmount + reemissionSettings.injectionBox.value
      } else {
        emissionAmount
      }
      val minersAssets = if (nextHeight >= eip27ActivationHeight) {
        // miner is getting reemission tokens
        assets.append(Colls.fromItems(reemissionTokenId -> reemissionAmount))
      } else {
        assets
      }
      val minerBox = new ErgoBoxCandidate(minerAmt, minerProp, nextHeight, minersAssets)

      val emissionTx = ErgoTransaction(
        inputs,
        dataInputs = IndexedSeq.empty,
        IndexedSeq(newEmissionBox, minerBox)
      )
      log.info(s"Emission tx for nextHeight = $nextHeight: $emissionTx")
      emissionTx
    }

    // forming transaction collecting tx fees
    val inputs = txs.flatMap(_.inputs)
    val feeBoxes: Seq[ErgoBox] = ErgoState
      .newBoxes(txs)
      .filter(b => java.util.Arrays.equals(b.propositionBytes, propositionBytes) && !inputs.exists(i => java.util.Arrays.equals(i.boxId, b.id)))
    val feeTxOpt: Option[ErgoTransaction] = if (feeBoxes.nonEmpty) {
      val feeAmount = feeBoxes.map(_.value).sum
      val feeAssets =
        feeBoxes.toArray.toColl.flatMap(_.additionalTokens).take(MaxAssetsPerBox)
      val inputs = feeBoxes.map(b => new Input(b.id, ProverResult.empty))
      val minerBox =
        new ErgoBoxCandidate(feeAmount, minerProp, nextHeight, feeAssets, Map())
      Some(ErgoTransaction(inputs.toIndexedSeq, IndexedSeq(), IndexedSeq(minerBox)))
    } else {
      None
    }

    Seq(emissionTxOpt, feeTxOpt).flatten
  }

  /**
    * Helper function which decides whether transactions can fit into a block with given cost and size limits
    */
  def correctLimits(
    blockTxs: Seq[CostedTransaction],
    maxBlockCost: Long,
    maxBlockSize: Long
  ): Boolean = {
    blockTxs.map(_._2).sum < maxBlockCost && blockTxs.map(_._1.size).sum < maxBlockSize
  }

  /**
    * Collects valid non-conflicting transactions from `mandatoryTxs` and then `mempoolTxsIn` and adds a transaction
    * collecting fees from them to `minerPk`.
    *
    * Resulting transactions total cost does not exceed `maxBlockCost`, total size does not exceed `maxBlockSize`,
    * and the miner's transaction is correct.
    *
    * @return - transactions to include into the block, transaction ids turned out to be invalid.
    */
  def collectTxs(
                  minerPk: ProveDlog,
                  maxBlockCost: Int,
                  maxBlockSize: Int,
                  us: UtxoStateReader,
                  upcomingContext: ErgoStateContext,
                  transactions: Seq[ErgoTransaction],
                  skippableOnOverflow: Set[ModifierId] = Set.empty
                ): (Seq[ErgoTransaction], Seq[ModifierId]) = {

    val currentHeight = us.stateContext.currentHeight
    val nextHeight = upcomingContext.currentHeight

    log.info(
      s"Assembling a block candidate for block #$nextHeight from ${transactions.length} transactions available"
    )

    val verifier: ErgoInterpreter = ErgoInterpreter(upcomingContext.currentParameters)

    // A candidate-local conflict does not prove that a mempool transaction is
    // invalid. Only context-free invalidity or an ERG imbalance against inputs
    // that exist in the base UTXO is sufficient to evict it here. In particular,
    // do not use script validation against this miner's tentative candidate as
    // a global mempool decision.
    def independentlyInvalid(tx: ErgoTransaction): Boolean = {
      if (tx.statelessValidity().isFailure) {
        true
      } else {
        val baseInputs = tx.inputs.map(input => us.boxById(input.boxId))
        baseInputs.forall(_.isDefined) && {
          val baseInputSum = Try(baseInputs.flatten.map(_.value).reduce(Math.addExact(_, _)))
          baseInputSum != tx.outputsSumTry
        }
      }
    }

    @tailrec
    def loop(
              mempoolTxs: Iterable[ErgoTransaction],
              acc: Seq[CostedTransaction],
              lastFeeTx: Option[CostedTransaction],
              invalidTxs: Seq[ModifierId]
            ): (Seq[ErgoTransaction], Seq[ModifierId]) = {
      // transactions from mempool and fee txs from the previous step
      val currentCosted = acc ++ lastFeeTx
      def current: Seq[ErgoTransaction] = currentCosted.map(_._1)

      val stateWithTxs = us.withTransactions(current)

      mempoolTxs.headOption match {
        case Some(tx) =>
          if (doublespend(current, tx)) {
            // A conflicting transaction can still be valid against the UTXO set. The
            // earlier transaction may be a speculative rent claim in this candidate.
            log.debug(s"Transaction ${tx.id} conflicts with an earlier candidate transaction")
            val rejected = if (independentlyInvalid(tx)) invalidTxs :+ tx.id else invalidTxs
            loop(mempoolTxs.tail, acc, lastFeeTx, rejected)
          } else if (!inputsNotSpent(tx, stateWithTxs)) {
            // Only an input absent independently of this candidate's prior spends is
            // a reason to evict the transaction from the mempool.
            log.debug(s"Transaction ${tx.id} spends non-existing inputs")
            loop(mempoolTxs.tail, acc, lastFeeTx, invalidTxs :+ tx.id)
          } else {
            // check validity and calculate transaction cost
            stateWithTxs.validateWithCost(
              tx,
              upcomingContext,
              maxBlockCost,
              Some(verifier)
            ) match {
              case Success(costConsumed) =>
                val newTxs = acc :+ (tx -> costConsumed)
                val newBoxes = newTxs.flatMap(_._1.outputs)

                collectFees(currentHeight, newTxs.map(_._1), minerPk, upcomingContext) match {
                  case Some(feeTx) =>
                    val boxesToSpend = feeTx.inputs.flatMap(i =>
                      newBoxes.find(b => java.util.Arrays.equals(b.id, i.boxId))
                    )
                    feeTx.statefulValidity(boxesToSpend, IndexedSeq(), upcomingContext)(verifier) match {
                      case Success(cost) =>
                        val blockTxs: Seq[CostedTransaction] = (feeTx -> cost) +: newTxs
                        if (correctLimits(blockTxs, maxBlockCost, maxBlockSize)) {
                          loop(mempoolTxs.tail, newTxs, Some(feeTx -> cost), invalidTxs)
                        } else {
                          if (skippableOnOverflow.contains(tx.id)) {
                            log.debug(s"Skipping speculative rent claim ${tx.id} on candidate limits overflow")
                            loop(mempoolTxs.tail, acc, lastFeeTx, invalidTxs)
                          } else {
                            log.debug(s"Finishing block assembly on limits overflow, " +
                              s"cost is ${currentCosted.map(_._2).sum}, cost limit: $maxBlockCost")
                            current -> invalidTxs
                          }
                        }
                      case Failure(e) =>
                        log.warn(
                          s"Fee collecting tx is invalid, not including it, " +
                            s"details: ${e.getMessage} from ${stateWithTxs.stateContext}"
                        )
                        current -> invalidTxs
                    }
                  case None =>
                    log.info(s"No fee proposition found in txs ${newTxs.map(_._1.id)} ")
                    val blockTxs: Seq[CostedTransaction] = newTxs ++ lastFeeTx.toSeq
                    if (correctLimits(blockTxs, maxBlockCost, maxBlockSize)) {
                      loop(mempoolTxs.tail, blockTxs, lastFeeTx, invalidTxs)
                    } else {
                      if (skippableOnOverflow.contains(tx.id))
                        loop(mempoolTxs.tail, acc, lastFeeTx, invalidTxs)
                      else current -> invalidTxs
                    }
                }
              case Failure(e) =>
                log.info(s"Not included transaction ${tx.id} due to ${e.getMessage}: ", e)
                loop(mempoolTxs.tail, acc, lastFeeTx, invalidTxs :+ tx.id)
            }
          }
        case None => // mempool is empty
          current -> invalidTxs
      }
    }

    val res = loop(transactions, Seq.empty, None, Seq.empty)
    log.debug(
      s"Collected ${res._1.length} transactions for block #$currentHeight, " +
        s"invalid transaction ids (total:${res._2.length}) for block #$currentHeight : ${res._2}")

    res
  }

  /** Checks that transaction "tx" is not spending outputs spent already by transactions "txs" */
  def doublespend(txs: Seq[ErgoTransaction], tx: ErgoTransaction): Boolean = {
    val txsInputs = txs.flatMap(_.inputs.map(_.boxId))
    tx.inputs.exists(i => txsInputs.exists(_.sameElements(i.boxId)))
  }

  /**
    * Derives header without pow from a block candidate provided
    */
  def deriveUnprovenHeader(candidate: CandidateBlock): HeaderWithoutPow = {
    val (parentId, height) = derivedHeaderFields(candidate.parentOpt)
    val transactionsRoot =
      BlockTransactions.transactionsRoot(candidate.transactions, candidate.version)
    val adProofsRoot = ADProofs.proofDigest(candidate.adProofBytes)
    val extensionRoot: Digest32 = candidate.extension.digest

    HeaderWithoutPow(
      candidate.version,
      parentId,
      adProofsRoot,
      candidate.stateRoot,
      transactionsRoot,
      candidate.timestamp,
      candidate.nBits,
      height,
      extensionRoot,
      candidate.votes,
      Array.emptyByteArray
    )
  }

  /**
    * Assemble `ErgoFullBlock` using candidate block and provided pow solution.
    */
  def completeBlock(candidate: CandidateBlock, solution: AutolykosSolution): ErgoFullBlock = {
    val header = deriveUnprovenHeader(candidate).toHeader(solution, None)
    val adProofs = ADProofs(header.id, candidate.adProofBytes)
    val blockTransactions = BlockTransactions(header.id, candidate.version, candidate.transactions)
    val extension = Extension(header.id, candidate.extension.fields)
    new ErgoFullBlock(header, blockTransactions, extension, Some(adProofs))
  }

}
