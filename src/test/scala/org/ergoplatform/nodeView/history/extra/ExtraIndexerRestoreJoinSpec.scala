package org.ergoplatform.nodeView.history.extra

import akka.actor.Props
import org.ergoplatform.consensus.ModifierSemanticValidity
import org.ergoplatform.core.idToVersion
import org.ergoplatform.nodeView.state.StateType
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.fixtures.NodeViewFixture
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock}
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestConfig, NodeViewTestOps}
import scorex.util.ModifierId

import scala.concurrent.duration._

class ExtraIndexerRestoreJoinSpec extends ErgoCorePropertyTest with NodeViewTestOps {

  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  property("repairs applied UTXO validity before the extra indexer catches up after restart") {
    val baseSettings = NodeViewTestConfig(
      StateType.Utxo,
      verifyTransactions = true,
      popowBootstrap = false
    ).toSettings
    val indexedSettings: ErgoSettings = baseSettings.copy(
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true)
    )

    new NodeViewFixture(indexedSettings, parameters).apply { fixture =>
      import fixture._

      val (sourceState, boxHolder) = createUtxoState(settings)
      try {
        val block = validFullBlock(None, sourceState, boxHolder)
        applyBlock(block).get
        org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
          getCurrentState.version shouldBe idToVersion(block.id)
        }
        getHistory.isSemanticallyValid(block.header.id) shouldBe ModifierSemanticValidity.Valid

        getHistory.historyStorage.remove(
          Array(getHistory.validityKey(block.header.id)),
          Array.empty[ModifierId]
        ).get
        getHistory.isSemanticallyValid(block.header.id) shouldBe ModifierSemanticValidity.Unknown

        stopNodeViewHolder()
        startNodeViewHolder()
        getHistory.isSemanticallyValid(block.header.id) shouldBe ModifierSemanticValidity.Valid

        val indexer = actorSystem.actorOf(Props(new ExtraIndexer(
          settings.cacheSettings,
          settings.chainSettings.addressEncoder,
          Some(nodeViewHolderRef)
        )))
        try {
          org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
            IndexerState.fromHistory(getHistory).indexedHeight shouldBe block.header.height
          }
        } finally {
          actorSystem.stop(indexer)
        }
      } finally {
        sourceState.closeStorage()
      }
    }
  }

  property("repairs applied Digest validity before the extra indexer catches up after restart") {
    val baseSettings = NodeViewTestConfig(
      StateType.Digest,
      verifyTransactions = true,
      popowBootstrap = false
    ).toSettings
    val indexedSettings: ErgoSettings = baseSettings.copy(
      nodeSettings = baseSettings.nodeSettings.copy(extraIndex = true)
    )

    new NodeViewFixture(indexedSettings, parameters).apply { fixture =>
      import fixture._

      val (sourceState, boxHolder) = createUtxoState(settings)
      try {
        val block = validFullBlock(None, sourceState, boxHolder)
        applyHeader(block.header).get
        getHistory.isSemanticallyValid(block.header.id) shouldBe ModifierSemanticValidity.Unknown
        applyPayload(block).get
        org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
          getCurrentState.version shouldBe idToVersion(block.id)
        }

        val appliedHistory = getHistory
        val validityIds = block.header.id +: block.header.sectionIds.map(_._2)
        validityIds.foreach { id =>
          appliedHistory.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Valid
        }
        IndexerState.fromHistory(appliedHistory).indexedHeight shouldBe 0

        // Recreate a crash after the Digest state commit but before the full block's
        // validity batch is written to history.
        appliedHistory.historyStorage.remove(
          validityIds.map(appliedHistory.validityKey).toArray,
          Array.empty[ModifierId]
        ).get
        validityIds.foreach { id =>
          appliedHistory.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Unknown
        }

        stopNodeViewHolder()
        startNodeViewHolder()
        getCurrentState.version shouldBe idToVersion(block.id)
        IndexerState.fromHistory(getHistory).indexedHeight shouldBe 0

        val indexer = actorSystem.actorOf(Props(new ExtraIndexer(
          settings.cacheSettings,
          settings.chainSettings.addressEncoder,
          Some(nodeViewHolderRef)
        )))
        try {
          org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
            IndexerState.fromHistory(getHistory).indexedHeight shouldBe block.header.height
          }
          validityIds.foreach { id =>
            getHistory.isSemanticallyValid(id) shouldBe ModifierSemanticValidity.Valid
          }
        } finally {
          actorSystem.stop(indexer)
        }
      } finally {
        sourceState.closeStorage()
      }
    }
  }
}
