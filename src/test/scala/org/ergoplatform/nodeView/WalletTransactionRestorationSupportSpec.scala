package org.ergoplatform.nodeView

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestProbe
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.RestoredTransaction
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.RequestCurrentWalletView
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages.{RegisterWalletTransactionRestoration, WalletTransactionsForRestoration}
import org.ergoplatform.utils.generators.ErgoNodeTransactionGenerators.validErgoTransactionGenTemplate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.ScorexLogging

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class WalletTransactionRestorationSupportSpec extends AnyFlatSpec with Matchers {
  private lazy val transaction = validErgoTransactionGenTemplate(minAssets = 0, maxInputs = 2).sample.get._2

  private def withHolder(test: (TestProbe, TestProbe, TestProbe, akka.actor.ActorRef, UUID) => Unit): Unit = {
    implicit val system: ActorSystem = ActorSystem("wallet-restoration-support")
    val wallet = TestProbe()
    val other = TestProbe()
    val observed = TestProbe()
    val holder = system.actorOf(Props(new Actor with WalletTransactionRestorationSupport with ScorexLogging {
      override protected def currentRestorationWalletActor: akka.actor.ActorRef = wallet.ref
      override protected def registerWalletTransactionRestoration(requestId: UUID): Unit =
        wallet.ref.tell(RegisterWalletTransactionRestoration(requestId), self)
      override def preStart(): Unit = beginWalletTransactionRestoration()
      override def receive: Receive = walletTransactionRestorationResponses.orElse {
        case RequestCurrentWalletView(_, replyTo) => refreshWalletTransactionRestoration(replyTo)
        case restored: RestoredTransaction => observed.ref ! restored
      }
    }))
    val request = wallet.expectMsgType[RegisterWalletTransactionRestoration](5.seconds)
    try test(wallet, other, observed, holder, request.requestId)
    finally Await.result(system.terminate(), 10.seconds)
  }

  "Wallet restoration responses" should "forward one matching response through the existing admission message" in {
    withHolder { (wallet, _, observed, holder, id) =>
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectMsgType[RestoredTransaction].unconfirmedTx.transaction.id shouldBe transaction.id
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectNoMessage(200.millis)
    }
  }

  it should "ignore a stale identifier without consuming the current registration" in {
    withHolder { (wallet, _, observed, holder, id) =>
      wallet.send(holder, WalletTransactionsForRestoration(UUID.randomUUID(), Success(Seq(transaction))))
      observed.expectNoMessage(200.millis)
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectMsgType[RestoredTransaction].unconfirmedTx.transaction.id shouldBe transaction.id
    }
  }

  it should "ignore a different sender without consuming the current registration" in {
    withHolder { (wallet, other, observed, holder, id) =>
      other.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectNoMessage(200.millis)
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectMsgType[RestoredTransaction].unconfirmedTx.transaction.id shouldBe transaction.id
    }
  }

  it should "consume a matching read failure without admitting records or retrying" in {
    withHolder { (wallet, _, observed, holder, id) =>
      wallet.send(holder, WalletTransactionsForRestoration(id, Failure(new IllegalStateException("read failed"))))
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectNoMessage(200.millis)
      wallet.expectNoMessage(200.millis)
    }
  }

  it should "repeat the same pending registration only for the current wallet view request" in {
    withHolder { (wallet, other, observed, holder, id) =>
      other.send(holder, RequestCurrentWalletView(UUID.randomUUID(), other.ref))
      wallet.expectNoMessage(200.millis)
      wallet.send(holder, RequestCurrentWalletView(UUID.randomUUID(), wallet.ref))
      wallet.expectMsg(RegisterWalletTransactionRestoration(id))
      wallet.send(holder, WalletTransactionsForRestoration(id, Success(Seq(transaction))))
      observed.expectMsgType[RestoredTransaction].unconfirmedTx.transaction.id shouldBe transaction.id
      wallet.send(holder, RequestCurrentWalletView(UUID.randomUUID(), wallet.ref))
      wallet.expectNoMessage(200.millis)
    }
  }
}
