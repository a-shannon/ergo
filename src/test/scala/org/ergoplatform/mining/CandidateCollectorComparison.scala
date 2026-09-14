package org.ergoplatform.mining

import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.nodeView.state.{ErgoStateContext, UtxoStateReader}
import org.ergoplatform.wallet.interpreter.ErgoInterpreter
import scorex.util.{ModifierId, ScorexLogging}
import sigma.data.ProveDlog
import scala.annotation.tailrec
import scala.util.{Failure, Success}

// Local comparison harness extracted from the pinned production collector.
// The accepted-only variant adds one guard; it is not an upstream fix.
object CandidateCollectorBaselineCopy extends ScorexLogging {
  import CandidateGenerator.{CostedTransaction, collectFees, correctLimits, doublespend}
  private def inputsNotSpent(tx: ErgoTransaction, state: UtxoStateReader): Boolean =
    tx.inputs.forall(input => state.boxById(input.boxId).isDefined)
  def collectTxs(
                  minerPk: ProveDlog,
                  maxBlockCost: Int,
                  maxBlockSize: Int,
                  us: UtxoStateReader,
                  upcomingContext: ErgoStateContext,
                  transactions: Seq[ErgoTransaction]
                ): (Seq[ErgoTransaction], Seq[ModifierId]) = {

    val currentHeight = us.stateContext.currentHeight
    val nextHeight = upcomingContext.currentHeight

    log.info(
      s"Assembling a block candidate for block #$nextHeight from ${transactions.length} transactions available"
    )

    val verifier: ErgoInterpreter = ErgoInterpreter(upcomingContext.currentParameters)

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
          if (!inputsNotSpent(tx, stateWithTxs) || doublespend(current, tx)) {
            //mark transaction as invalid if it tries to do double-spending or trying to spend outputs not present
            //do these checks before validating the scripts to save time
            log.debug(s"Transaction ${tx.id} double-spending or spending non-existing inputs")
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
                          log.debug(s"Finishing block assembly on limits overflow, " +
                                    s"cost is ${currentCosted.map(_._2).sum}, cost limit: $maxBlockCost")
                          current -> invalidTxs
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
                      current -> invalidTxs
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
}

object CandidateCollectorAcceptedOnly extends ScorexLogging {
  import CandidateGenerator.{CostedTransaction, collectFees, correctLimits, doublespend}
  private def inputsNotSpent(tx: ErgoTransaction, state: UtxoStateReader): Boolean =
    tx.inputs.forall(input => state.boxById(input.boxId).isDefined)
  def collectTxs(
                  minerPk: ProveDlog,
                  maxBlockCost: Int,
                  maxBlockSize: Int,
                  us: UtxoStateReader,
                  upcomingContext: ErgoStateContext,
                  transactions: Seq[ErgoTransaction]
                ): (Seq[ErgoTransaction], Seq[ModifierId]) = {

    val currentHeight = us.stateContext.currentHeight
    val nextHeight = upcomingContext.currentHeight

    log.info(
      s"Assembling a block candidate for block #$nextHeight from ${transactions.length} transactions available"
    )

    val verifier: ErgoInterpreter = ErgoInterpreter(upcomingContext.currentParameters)

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
        case Some(tx) if acc.exists(_._1.id == tx.id) =>
          loop(mempoolTxs.tail, acc, lastFeeTx, invalidTxs)
        case Some(tx) =>
          if (!inputsNotSpent(tx, stateWithTxs) || doublespend(current, tx)) {
            //mark transaction as invalid if it tries to do double-spending or trying to spend outputs not present
            //do these checks before validating the scripts to save time
            log.debug(s"Transaction ${tx.id} double-spending or spending non-existing inputs")
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
                          log.debug(s"Finishing block assembly on limits overflow, " +
                                    s"cost is ${currentCosted.map(_._2).sum}, cost limit: $maxBlockCost")
                          current -> invalidTxs
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
                      current -> invalidTxs
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
}
