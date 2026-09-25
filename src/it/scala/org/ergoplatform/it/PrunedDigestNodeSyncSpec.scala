package org.ergoplatform.it

import java.io.File
import akka.japi.Option.Some
import com.typesafe.config.Config
import io.circe.{Decoder, Json}
import io.circe.parser.decode
import org.asynchttpclient.util.HttpConstants
import org.ergoplatform.it.api.NodeApi
import org.ergoplatform.it.container.{IntegrationSuite, Node}
import org.scalatest.flatspec.AnyFlatSpec

import scala.async.Async
import scala.concurrent.Await
import scala.concurrent.duration._

class PrunedDigestNodeSyncSpec extends AnyFlatSpec with IntegrationSuite {

  val approxTargetHeight = 10
  val blocksToKeep: Int = approxTargetHeight / 5

  val localVolume = s"$localDataDir/digest-node-sync-spec/data"
  val remoteVolume = "/app"

  val dir = new File(localVolume)
  dir.mkdirs()

  val minerConfig: Config = nodeSeedConfigs.head
    // NB: left at the original slow interval on purpose — this spec's pruning assertion
    // (a low block must already be pruned right after sync) depends on the slow mining cadence;
    // faster mining makes the one-shot check race ahead of asynchronous pruning.
    .withFallback(internalMinerPollingIntervalConfig(10000))
    .withFallback(specialDataDirConfig(remoteVolume))
    .withFallback(allowLocalConfig)

  val nodeForSyncingConfig: Config = minerConfig
    .withFallback(nonGeneratingPeerConfig)
    .withFallback(allowLocalConfig)

  val digestConfig: Config = digestStatePeerConfig
    .withFallback(blockIntervalConfig(500))
    .withFallback(prunedHistoryConfig(blocksToKeep))
    .withFallback(nonGeneratingPeerConfig)
    .withFallback(nodeSeedConfigs(1))
    .withFallback(allowLocalConfig)

  // Testing scenario:
  // 1. Start up mining node and let it mine chain of length ~ {approxTargetHeight};
  // 2. Shut it down, restart with turned off mining and fetch its info to get actual {targetHeight};
  // 3. Start digest node and wait until it gets synced with the first one up to {targetHeight} ensuring
  //    it does not load full block that should be pruned;
  // 4. Fetch digest node info and compare it with first node's one;
  // 5. Make sure digest node does not store full blocks with height < {targetHeight - blocksToKeep};
  it should "Pruned digest node synchronization" in {

    val minerNode: Node = docker.startDevNetNode(minerConfig, specialVolumeOpt = Some((localVolume, remoteVolume))).get

    val result = Async.async {
      Async.await(minerNode.waitForHeight(approxTargetHeight, 1.second))
      docker.stopNode(minerNode, secondsToWait = 0)

      val nodeForSyncing = docker
        .startDevNetNode(nodeForSyncingConfig, specialVolumeOpt = Some((localVolume, remoteVolume))).get
      Async.await(nodeForSyncing.waitForHeight(approxTargetHeight))
      val sampleSnapshot = Async.await(nodeForSyncing.get("/info")
        .map(r => PrunedDigestInfo.decodeSnapshot(r.getResponseBody)))
      val sampleInfo = sampleSnapshot.info

      val digestNode = docker.startDevNetNode(digestConfig).get
      val targetHeight = sampleInfo.bestBlockHeightOpt.value
      val targetBlockId = sampleInfo.bestBlockIdOpt.value
      val blocksToPrune = Async.await(nodeForSyncing.headers(0, targetHeight - blocksToKeep - 3))
      Async.await(digestNode.waitFor[Option[String]](
        _.info.map(_.bestBlockIdOpt),
        blockIdOpt => {
          blockIdOpt.foreach(blocksToPrune should not contain _)
          blockIdOpt.contains(targetBlockId)
        },
        50.millis
      ))
      val digestSnapshot = Async.await(digestNode.get("/info")
        .map(r => PrunedDigestInfo.decodeSnapshot(r.getResponseBody)))
      val digestNodeInfo = digestSnapshot.info
      withClue(PrunedDigestInfo.comparisonClue(sampleSnapshot, digestSnapshot)) {
        digestNodeInfo shouldEqual sampleInfo
      }
      Async.await(digestNode.singleGet(s"/blocks/${blocksToPrune.last}")
        .map(_.getStatusCode == HttpConstants.ResponseStatusCodes.OK_200)) shouldBe false
    }

    Await.result(result, 10.minutes)
  }

}

/** Retain diagnostic fields from the same responses used by the original assertion. */
private[it] object PrunedDigestInfo {
  final case class Snapshot(info: NodeApi.NodeInfo, stateVersion: Option[Json])

  private implicit val snapshotDecoder: Decoder[Snapshot] = { cursor =>
    // A malformed diagnostic field must not replace the original equality failure.
    NodeApi.nodeInfoDecoder(cursor).map(info =>
      Snapshot(info, cursor.downField("stateVersion").focus))
  }

  def decodeSnapshot(body: String): Snapshot =
    decode[Snapshot](body).fold(throw _, identity)

  def comparisonClue(sample: Snapshot, digest: Snapshot): String = {
    def version(snapshot: Snapshot): String =
      snapshot.stateVersion.fold("missing")(_.noSpaces)

    s"Original /info samples: source stateVersion=${version(sample)}, " +
      s"digest stateVersion=${version(digest)}. "
  }
}
