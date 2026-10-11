package org.ergoplatform.nodeView.history.storage

import java.io.File
import java.util.UUID

import akka.japi.Option.Some
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import org.ergoplatform.it.container.{IntegrationSuite, Node}
import org.ergoplatform.modifiers.history.{BlockTransactions, HistoryModifierSerializer}
import org.ergoplatform.nodeView.history.extra.ExtraIndexer
import org.ergoplatform.settings.Algos
import org.scalatest.flatspec.AnyFlatSpec
import scorex.db.LDBFactory
import scorex.util.idToBytes

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/** Exercises the pending history insertion across a real node-container/JVM restart. */
class HistoryInsertionProcessRestartSpec extends AnyFlatSpec with IntegrationSuite {

  private val localVolume = s"$localDataDir/history-insertion-process-restart/data"
  private val remoteVolume = "/app"

  private val commonConfig: Config = ConfigFactory.parseString("ergo.node.extraIndex = true")
    .withFallback(specialDataDirConfig(remoteVolume))
    .withFallback(nodeSeedConfigs.head)
    .withFallback(allowLocalConfig)

  private val miningConfig: Config = offlineGeneratingPeerConfig.withFallback(commonConfig)
  private val recoveryConfig: Config = nonGeneratingPeerConfig.withFallback(commonConfig)

  private def json(node: Node, path: String): Future[Json] =
    node.get(path).map(response => node.ergoJsonAnswerAs[Json](response.getResponseBody))

  private def appliedFullBlock(node: Node): Future[Option[(Int, String)]] =
    json(node, "/info").map { response =>
      val cursor = response.hcursor
      for {
        height <- cursor.get[Option[Int]]("fullHeight").getOrElse(None).filter(_ > 0)
        blockId <- cursor.get[Option[String]]("bestFullHeaderId").getOrElse(None)
        stateVersion <- cursor.get[Option[String]]("stateVersion").getOrElse(None)
        if stateVersion == blockId
      } yield height -> blockId
    }

  it should "replay a pending header and index row before resuming extra indexing in a new JVM" in {
    val dataDir = new File(localVolume)
    (dataDir.isDirectory || dataDir.mkdirs()) shouldBe true
    val miner = docker.startDevNetNode(
      miningConfig, specialVolumeOpt = Some((localVolume, remoteVolume))).get

    val (selectedHeight, selectedId) = Await.result(
      miner.waitFor[Option[(Int, String)]](
        n => appliedFullBlock(n),
        (value: Option[(Int, String)]) => value.isDefined,
        200.millis), 3.minutes).get
    val selectedHeader = Await.result(miner.headerById(selectedId), 30.seconds)
    selectedHeader.height shouldBe selectedHeight
    selectedHeader.encodedId shouldBe selectedId

    // Closing the first container releases every LevelDB lock before offline mutation.
    docker.forceStopNode(miner.containerId)
    docker.allowHostAccessToMountedVolume(localVolume, remoteVolume)

    val headerKey = selectedHeader.serializedId
    val headerBytesAndTxId = {
      val objects = LDBFactory.createKvDb(s"$localVolume/history/objects")
      try {
        val headerBytes = objects.get(headerKey).getOrElse(fail("selected header is absent"))
        HistoryModifierSerializer.parseBytesTry(headerBytes).get.id shouldBe selectedHeader.id
        val transactionsBytes = objects.get(idToBytes(selectedHeader.transactionsId))
          .getOrElse(fail("selected full block has no stored transactions"))
        val transactions = HistoryModifierSerializer.parseBytesTry(transactionsBytes).get
          .asInstanceOf[BlockTransactions]
        transactions.headerId shouldBe selectedHeader.id
        val txId = transactions.txs.last.id
        objects.remove(Array(headerKey)).get
        objects.get(headerKey) shouldBe None
        headerBytes -> txId
      } finally objects.close()
    }
    val (headerBytes, txId) = headerBytesAndTxId

    val sentinelKey = Algos.hash(s"process-restart-sentinel-${UUID.randomUUID()}")
    val sentinelValue = Array[Byte](0x51, 0x27, 0x19)
    val intent = HistoryInsertionJournal.Intent(
      Vector(headerKey -> headerBytes), Vector(sentinelKey -> sentinelValue))
    val index = LDBFactory.createKvDb(s"$localVolume/history/index")
    try {
      index.get(sentinelKey) shouldBe None
      index.get(HistoryInsertionJournal.key) shouldBe None
      index.insert(HistoryInsertionJournal.key, HistoryInsertionJournal.encode(intent)).get
      HistoryInsertionJournal.decode(index.get(HistoryInsertionJournal.key).get)
        .objects.head._1.toSeq shouldBe headerKey.toSeq
    } finally index.close()

    val extra = LDBFactory.createKvDb(s"$localVolume/history/extra")
    try extra.get(ExtraIndexer.SchemaVersionKey).get.toSeq shouldBe
      ExtraIndexer.NewestVersionBytes.toSeq
    finally extra.close()

    val restarted = docker.startDevNetNode(
      recoveryConfig, specialVolumeOpt = Some((localVolume, remoteVolume))).get
    restarted.containerId should not be miner.containerId
    Await.result(restarted.waitForStartup, 90.seconds)
    Await.result(restarted.info, 20.seconds).isMining shouldBe Some(false)

    val restoredHeader = Await.result(restarted.headerById(selectedId), 30.seconds)
    restoredHeader.encodedId shouldBe selectedId

    val indexed = Await.result(restarted.waitFor[Json](
      n => json(n, "/blockchain/indexedHeight"),
      (response: Json) => {
        val cursor = response.hcursor
        val indexedHeight = cursor.get[Int]("indexedHeight").getOrElse(-1)
        val fullHeight = cursor.get[Int]("fullHeight").getOrElse(-1)
        indexedHeight >= selectedHeight && indexedHeight == fullHeight
      },
      200.millis), 3.minutes)
    indexed.hcursor.get[Int]("indexedHeight").getOrElse(-1) should be >= selectedHeight

    val indexedTx = Await.result(
      json(restarted, s"/blockchain/transaction/byId/$txId"), 30.seconds)
    indexedTx.hcursor.get[String]("id").fold(e => fail(e.message), identity) shouldBe txId
    indexedTx.hcursor.get[String]("blockId").fold(e => fail(e.message), identity) shouldBe selectedId
    indexedTx.hcursor.get[Int]("inclusionHeight").fold(e => fail(e.message), identity) shouldBe
      selectedHeight

    docker.forceStopNode(restarted.containerId)
    docker.allowHostAccessToMountedVolume(localVolume, remoteVolume)

    val restoredObjects = LDBFactory.createKvDb(s"$localVolume/history/objects")
    try restoredObjects.get(headerKey).get.toSeq shouldBe headerBytes.toSeq
    finally restoredObjects.close()

    val restoredIndex = LDBFactory.createKvDb(s"$localVolume/history/index")
    try {
      restoredIndex.get(sentinelKey).get.toSeq shouldBe sentinelValue.toSeq
      restoredIndex.get(HistoryInsertionJournal.key) shouldBe None
    } finally restoredIndex.close()

    val restoredExtra = LDBFactory.createKvDb(s"$localVolume/history/extra")
    try restoredExtra.get(ExtraIndexer.SchemaVersionKey).get.toSeq shouldBe
      ExtraIndexer.NewestVersionBytes.toSeq
    finally restoredExtra.close()
  }
}
