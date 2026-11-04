package org.ergoplatform.nodeView.wallet

import java.util.Arrays

import org.ergoplatform.{ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.ChangedMempool
import org.ergoplatform.nodeView.mempool.ErgoMemPool
import org.ergoplatform.settings.Constants.TrueTree
import org.ergoplatform.utils.{ErgoCorePropertyTest, WalletTestOps}
import org.ergoplatform.wallet.Constants.PaymentsScanId
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

class OffchainParentRecoverySpec extends ErgoCorePropertyTest with WalletTestOps with Eventually {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  property("a retained unconfirmed parent output returns to wallet and Payments scan after child removal") {
    withFixture { implicit fixture =>
      val address = getPublicKeys.head
      val value = settings.walletSettings.dustLimit.getOrElse(1000000L) + 1000000L
      val parent = ErgoTransaction(fakeInputs, IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(value, address.script, startHeight)))
      val parentBox = parent.outputs.head
      val child = ErgoTransaction(
        IndexedSeq(Input(parentBox.id, emptyProverResult)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(value, TrueTree, startHeight)))

      val parentPool = ErgoMemPool.empty(settings).put(UnconfirmedTransaction(parent, None))
      val bothPool = parentPool.put(UnconfirmedTransaction(child, None))
      val afterChildRemoval = bothPool.invalidate(child.id)
      parentPool.modifierById(parent.id).isDefined shouldBe true
      bothPool.modifierById(child.id).isDefined shouldBe true
      afterChildRemoval.modifierById(parent.id).isDefined shouldBe true
      afterChildRemoval.modifierById(child.id) shouldBe None
      afterChildRemoval.spentInputs.exists(id => Arrays.equals(id, parentBox.id)) shouldBe false

      def inWallet: Boolean = await(wallet.walletBoxes(unspentOnly = true,
        considerUnconfirmed = true)).exists(b => Arrays.equals(b.trackedBox.box.id, parentBox.id))
      def inPaymentsScan: Boolean = await(wallet.scanUnspentBoxes(PaymentsScanId,
        true, 0, Int.MaxValue))
        .exists(b => Arrays.equals(b.trackedBox.box.id, parentBox.id))

      implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 100.millis)
      wallet.walletActor ! ChangedMempool(parentPool)
      eventually {
        (inWallet, inPaymentsScan) shouldBe ((true, true))
      }

      wallet.walletActor ! ChangedMempool(bothPool)
      eventually {
        (inWallet, inPaymentsScan) shouldBe ((false, false))
      }

      wallet.walletActor ! ChangedMempool(afterChildRemoval)
      eventually {
        (inWallet, inPaymentsScan) shouldBe ((true, true))
      }
    }
  }
}
