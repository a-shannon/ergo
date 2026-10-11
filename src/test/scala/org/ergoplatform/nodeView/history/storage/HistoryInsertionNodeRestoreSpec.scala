package org.ergoplatform.nodeView.history.storage

import java.io.File

import akka.actor.Props
import org.ergoplatform.consensus.ModifierSemanticValidity
import org.ergoplatform.core.idToVersion
import org.ergoplatform.modifiers.history.HistoryModifierSerializer
import org.ergoplatform.nodeView.history.extra.{ExtraIndexer, IndexerState}
import org.ergoplatform.nodeView.state.StateType
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.fixtures.NodeViewFixture
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock}
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestConfig, NodeViewTestOps}
import scorex.db.{ByteArrayWrapper, LDBFactory}

import scala.concurrent.duration._

class HistoryInsertionNodeRestoreSpec extends ErgoCorePropertyTest with NodeViewTestOps {

  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  property("node restart replays a pending core insertion and resumes the release extra indexer") {
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

        stopNodeViewHolder()
        val indexKey = Array.fill[Byte](32)(43)
        val intent = HistoryInsertionJournal.Intent(
          Vector(block.header.serializedId -> HistoryModifierSerializer.toBytes(block.header)),
          Vector(indexKey -> Array[Byte](8))
        )
        val objects = LDBFactory.createKvDb(new File(nodeViewDir, "history/objects").getPath)
        try {
          objects.remove(Array(block.header.serializedId)).get
          objects.get(block.header.serializedId) shouldBe None
        } finally objects.close()
        val index = LDBFactory.createKvDb(new File(nodeViewDir, "history/index").getPath)
        try index.insert(HistoryInsertionJournal.key, HistoryInsertionJournal.encode(intent)).get
        finally index.close()
        val extra = LDBFactory.createKvDb(new File(nodeViewDir, "history/extra").getPath)
        try extra.insert(ExtraIndexer.SchemaVersionKey, ExtraIndexer.NewestVersionBytes).get
        finally extra.close()

        startNodeViewHolder()
        getHistory.isSemanticallyValid(block.header.id) shouldBe ModifierSemanticValidity.Valid
        getHistory.modifierById(block.header.id).map(_.id) shouldBe Some(block.header.id)
        getHistory.historyStorage.getIndex(ByteArrayWrapper(indexKey)).get.toSeq shouldBe Seq[Byte](8)

        val indexer = actorSystem.actorOf(Props(new ExtraIndexer(
          settings.cacheSettings,
          settings.chainSettings.addressEncoder,
          Some(nodeViewHolderRef)
        )))
        try {
          org.ergoplatform.utils.untilTimeout(10.seconds, 50.millis) {
            IndexerState.fromHistory(getHistory).indexedHeight shouldBe block.header.height
          }
        } finally actorSystem.stop(indexer)
      } finally sourceState.closeStorage()
    }
  }
}
