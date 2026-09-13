package org.ergoplatform.nodeView

import java.util.UUID

import akka.actor.{Actor, ActorRef}
import org.ergoplatform.modifiers.mempool.UnconfirmedTransaction
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.RestoredTransaction
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages.WalletTransactionsForRestoration
import scorex.util.ScorexLogging

import scala.util.{Failure, Success}

/** One startup registration, fulfilled only by the current wallet actor once it is operational. */
private[nodeView] trait WalletTransactionRestorationSupport { this: Actor with ScorexLogging =>
  private val walletRestorationRequestId: UUID = UUID.randomUUID()
  private var walletRestorationCompleted: Boolean = false

  protected def currentRestorationWalletActor: ActorRef

  protected def registerWalletTransactionRestoration(requestId: UUID): Unit

  protected final def beginWalletTransactionRestoration(): Unit =
    if (!walletRestorationCompleted) registerWalletTransactionRestoration(walletRestorationRequestId)

  protected final def refreshWalletTransactionRestoration(walletActor: ActorRef): Unit =
    if (walletActor == currentRestorationWalletActor) beginWalletTransactionRestoration()

  protected final def walletTransactionRestorationResponses: Receive = {
    case WalletTransactionsForRestoration(requestId, result)
      if !walletRestorationCompleted && requestId == walletRestorationRequestId &&
        sender() == currentRestorationWalletActor =>
      walletRestorationCompleted = true
      result match {
        case Success(txs) =>
          log.info(s"Restoring ${txs.size} unconfirmed wallet transaction(s) after wallet recovery")
          txs.foreach(tx => self ! RestoredTransaction(UnconfirmedTransaction(tx, None)))
        case Failure(t) =>
          log.warn("Could not read unconfirmed wallet transactions to restore: ", t)
      }

    case _: WalletTransactionsForRestoration =>
      log.debug("Ignoring an unexpected or completed wallet restoration response")
  }
}
