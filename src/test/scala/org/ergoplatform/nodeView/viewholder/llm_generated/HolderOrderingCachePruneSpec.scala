package org.ergoplatform.nodeView.viewholder.llm_generated

import org.ergoplatform.nodeView.state.StateType
import org.ergoplatform.nodeView.state.wrapped.WrappedUtxoState
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestConfig, NodeViewTestOps}
import org.ergoplatform.utils.fixtures.NodeViewFixture
import org.ergoplatform.utils.generators.ValidBlocksGenerators.createUtxoState
import org.ergoplatform.utils.generators.ValidBlocksGenerators.validFullBlock

class HolderOrderingCachePruneSpec extends ErgoCorePropertyTest with NodeViewTestOps {
  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  property("ordinary full-block progress prunes prior ordering transactions") {
    val settings = NodeViewTestConfig(
      StateType.Utxo, verifyTransactions = true, popowBootstrap = false).toSettings

    new NodeViewFixture(settings, parameters).apply { fixture =>
      import fixture._
      val (utxoState, boxes) = createUtxoState(fixture.settings)
      try {
        val genesis = validFullBlock(None, utxoState, boxes)
        val afterGenesis = WrappedUtxoState(utxoState, boxes, fixture.settings)
          .applyModifier(genesis)(_ => ()).get
        val successor = validFullBlock(Some(genesis), afterGenesis)
        successor.header.height shouldBe genesis.header.height + 1

        applyBlock(genesis).isSuccess shouldBe true
        val firstHistory = getHistory
        firstHistory.bestFullBlockOpt.map(_.header.id) shouldBe Some(genesis.header.id)
        val cached = genesis.blockTransactions.transactions
        firstHistory.saveOrderingBlockTransactions(genesis.id, cached)
        firstHistory.getOrderingBlockTransactions(genesis.id) shouldBe Some(cached)
        firstHistory.heightOf(genesis.id) shouldBe Some(genesis.header.height)

        applyBlock(successor).isSuccess shouldBe true
        // The actor query follows the Holder messages, including their cache updates.
        val advanced = getHistory
        advanced.bestFullBlockOpt.map(_.header.id) shouldBe Some(successor.header.id)
        advanced.heightOf(genesis.id) shouldBe Some(genesis.header.height)
        val afterHolder = advanced.getOrderingBlockTransactions(genesis.id)

        // Verify the existing pruning rule separately from its Holder trigger.
        advanced.saveOrderingBlockTransactions(genesis.id, cached)
        advanced.getOrderingBlockTransactions(genesis.id) shouldBe Some(cached)
        advanced.updateStateWithOrderingBlock(successor.header)
        advanced.getOrderingBlockTransactions(genesis.id) shouldBe None

        withClue("HOLDER_PRUNE_FINAL: ") {
          afterHolder shouldBe None
        }
      } finally {
        utxoState.closeStorage()
      }
    }
  }
}
