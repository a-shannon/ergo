package org.ergoplatform.nodeView.history

import akka.actor.{ActorRef, Props}
import akka.testkit.TestProbe
import org.ergoplatform.mining.AutolykosPowScheme
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.ChangedState
import org.ergoplatform.nodeView.history.storage.HistoryStorage
import org.ergoplatform.nodeView.history.storage.modifierprocessors.FullBlockSectionProcessor
import org.ergoplatform.nodeView.wallet.{ErgoWalletActor, ErgoWalletServiceImpl}
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages.{CloseWallet, GetWalletStatus, ScanOnChain, WalletStatus}
import org.ergoplatform.nodeView.wallet.persistence.WalletRegistry
import org.ergoplatform.nodeView.wallet.requests.PaymentRequest
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.{ErgoCorePropertyTest, WalletTestOps}
import org.ergoplatform.utils.fixtures.WalletFixture
import org.ergoplatform.wallet.boxes.ReplaceCompactCollectBoxSelector
import org.scalatest.concurrent.Eventually

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class WalletMissingFullBlockScanSpec extends ErgoCorePropertyTest with WalletTestOps with Eventually {
  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  private case class ScanActor(ref: ActorRef,
                               probe: TestProbe,
                               settings: ErgoSettings,
                               readsAtSecond: AtomicInteger,
                               readsAtThird: AtomicInteger)

  private def status(scan: ScanActor): WalletStatus = {
    scan.probe.send(scan.ref, GetWalletStatus)
    scan.probe.expectMsgType[WalletStatus]
  }

  private def close(scan: ScanActor): Unit = {
    scan.probe.watch(scan.ref)
    scan.probe.send(scan.ref, CloseWallet)
    scan.probe.expectTerminated(scan.ref, 5.seconds)
  }

  private def durableHeight(scan: ScanActor): Int = {
    val registry = WalletRegistry(scan.settings).get
    try registry.fetchDigest().height finally registry.close()
  }

  property("ordinary catch-up retains the wallet height when the next full block is absent") {
    withFixture { implicit w: WalletFixture =>
      val address = getPublicKeys.head
      val first = makeGenesisBlock(address.pubkey)
      applyBlock(first) shouldBe 'success

      def selfPayment(expectedHeight: Int) = {
        val balance = eventually(timeout(10.seconds), interval(100.millis)) {
          val digest = getConfirmedBalances
          digest.height shouldBe expectedHeight
          digest.walletBalance should be > 0L
          digest.walletBalance
        }
        val payment = PaymentRequest(address, balance / 2, Array.empty, Map.empty)
        await(wallet.generateTransaction(Seq(payment))).get
      }

      val second = makeNextBlock(getUtxoState, Seq(selfPayment(first.height)))
      applyBlock(second) shouldBe 'success
      val third = makeNextBlock(getUtxoState, Seq(selfPayment(second.height)))
      applyBlock(third) shouldBe 'success

      val source = getHistory
      source.bestFullBlockAt(second.height) should not be None
      source.bestFullBlockAt(third.height) should not be None

      def newScanActor(name: String, hideSecond: Boolean): ScanActor = {
        val actorSettings = w.settings.copy(
          directory = new File(w.nodeViewDir, name).getAbsolutePath,
          nodeSettings = w.settings.nodeSettings.copy(blocksToKeep = 1)
        )
        val secondReads = new AtomicInteger()
        val thirdReads = new AtomicInteger()
        val historyView = new ErgoHistory with FullBlockSectionProcessor {
          override protected val settings: ErgoSettings = actorSettings
          override protected[history] val historyStorage: HistoryStorage = source.historyStorage
          override val powScheme: AutolykosPowScheme = actorSettings.chainSettings.powScheme

          override def bestFullBlockAt(height: Int): Option[ErgoFullBlock] = {
            if (height == second.height) secondReads.incrementAndGet()
            if (height == third.height) thirdReads.incrementAndGet()
            if (hideSecond && height == second.height) None else super.bestFullBlockAt(height)
          }
        }
        val walletSettings = actorSettings.walletSettings
        val selector = new ReplaceCompactCollectBoxSelector(
          walletSettings.maxInputs, walletSettings.optimalInputs, None
        )
        val service = new ErgoWalletServiceImpl(actorSettings)
        val ref = w.actorSystem.actorOf(
          Props(classOf[ErgoWalletActor], actorSettings, parameters, service, selector, historyView)
        )
        ScanActor(ref, TestProbe()(w.actorSystem), actorSettings, secondReads, thirdReads)
      }

      val control = newScanActor("available", hideSecond = false)
      val missing = newScanActor("missing", hideSecond = true)
      try {
        Seq(control, missing).foreach { scan =>
          scan.probe.send(scan.ref, ChangedState(getCurrentState))
          scan.probe.send(scan.ref, ScanOnChain(first))
          eventually(timeout(10.seconds), interval(100.millis)) {
            val seeded = status(scan)
            seeded.height shouldBe first.height
            seeded.error shouldBe None
          }
        }

        control.probe.send(control.ref, ScanOnChain(third))
        eventually(timeout(10.seconds), interval(100.millis)) {
          status(control).height shouldBe third.height
        }
        control.readsAtSecond.get should be >= 1
        control.readsAtThird.get should be >= 1

        missing.probe.send(missing.ref, ScanOnChain(third))
        eventually(timeout(10.seconds), interval(100.millis)) {
          missing.readsAtSecond.get should be >= 1
        }
        // The first reply may precede self h3; the next follows it in the mailbox.
        status(missing)
        val stalled = status(missing)
        stalled.height shouldBe first.height
        stalled.error shouldBe None
        missing.readsAtSecond.get shouldBe 1
        missing.readsAtThird.get shouldBe 0

        missing.probe.send(missing.ref, ScanOnChain(third))
        eventually(timeout(10.seconds), interval(100.millis)) {
          missing.readsAtSecond.get should be >= 2
        }
        status(missing)
        val retried = status(missing)
        retried.height shouldBe first.height
        retried.error shouldBe None
        missing.readsAtThird.get shouldBe 0
      } finally {
        try close(control) finally close(missing)
      }

      durableHeight(control) shouldBe third.height
      durableHeight(missing) shouldBe first.height
    }
  }
}
