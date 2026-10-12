package org.ergoplatform.nodeView.history.extra

import akka.actor.Props
import org.ergoplatform.consensus.ModifierSemanticValidity
import org.ergoplatform.core.idToVersion
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.nodeView.state.{DigestState, ErgoState, StateType}
import org.ergoplatform.nodeView.state.wrapped.WrappedUtxoState
import org.ergoplatform.utils.fixtures.NodeViewFixture
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock}
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestConfig, NodeViewTestOps}
import scorex.util.ModifierId

import scala.concurrent.duration._

class ExtraIndexerDigestPrefixSpec extends ErgoCorePropertyTest with NodeViewTestOps {
  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  property("Digest header-only recovery does not certify an unapplied full block for indexing") {
    val defaults = NodeViewTestConfig(
      StateType.Digest,
      verifyTransactions = true,
      popowBootstrap = false
    ).toSettings
    val settings = defaults.copy(nodeSettings = defaults.nodeSettings.copy(extraIndex = true))

    new NodeViewFixture(settings, parameters).apply { fixture =>
      import fixture._

      val (utxoState, boxes) = createUtxoState(fixture.settings)
      try {
        var source = WrappedUtxoState(utxoState, boxes, fixture.settings)
        var parent: Option[ErgoFullBlock] = None
        val blocks = (1 to 2).map { _ =>
          val block = validFullBlock(parent, source)
          source = source.applyModifier(block)(_ => ()).get
          applyBlock(block).get
          parent = Some(block)
          block
        }
        val first = blocks.head
        val second = blocks.last

        // Model a crash after history selected a complete second block but before its
        // state application/validity batch. The second block's bytes stay in history.
        val history = getHistory
        val secondIds = second.header.id +: second.header.sectionIds.map(_._2)
        org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
          getCurrentState.version shouldBe idToVersion(second.id)
          secondIds.foreach { id =>
            history.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Valid
          }
        }
        history.bestFullBlockIdOpt shouldBe Some(second.id)
        history.historyStorage.remove(
          secondIds.map(history.validityKey).toArray,
          Array.empty[ModifierId]
        ).get
        secondIds.foreach { id =>
          history.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Unknown
        }

        stopNodeViewHolder()
        val stateDirectory = ErgoState.stateDir(fixture.settings)
        val persisted = DigestState.create(None, None, stateDirectory, fixture.settings)
        try {
          persisted.rollbackTo(idToVersion(first.id)).get.version shouldBe idToVersion(first.id)
        } finally {
          persisted.close()
        }

        // The production startup recovery uses headers only, advancing the Digest
        // version/root to the selected full tip without checking its transactions.
        startNodeViewHolder()
        getCurrentState.version shouldBe idToVersion(second.id)
        getCurrentState.rootDigest shouldEqual second.header.stateRoot
        getHistory.bestFullBlockIdOpt shouldBe Some(second.id)
        secondIds.foreach { id =>
          getHistory.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Unknown
        }

        val indexer = actorSystem.actorOf(Props(new ExtraIndexer(
          fixture.settings.cacheSettings,
          fixture.settings.chainSettings.addressEncoder,
          Some(nodeViewHolderRef),
          fixture.settings.nodeSettings.storageRentCollection
        )))
        try {
          org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
            IndexerState.fromHistory(getHistory).indexedHeight shouldBe first.header.height
          }
          // Give the indexer's one-second retry timer time to persist any later block.
          Thread.sleep(1500)
          IndexerState.fromHistory(getHistory).indexedHeight shouldBe first.header.height
          IndexerState.fromHistory(getHistory).indexedHeaderId shouldBe Some(first.id)
        } finally {
          actorSystem.stop(indexer)
        }
      } finally {
        utxoState.closeStorage()
      }
    }
  }
}
