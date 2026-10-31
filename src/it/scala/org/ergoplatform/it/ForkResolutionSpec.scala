package org.ergoplatform.it

import java.io.File
import cats.implicits._
import com.typesafe.config.Config
import org.ergoplatform.it.api.NodeApi.NodeInfo
import org.ergoplatform.it.container.Docker.{ExtraConfig, noExtraConfig}
import org.ergoplatform.it.container.{IntegrationSuite, Node}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.async.Async
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class ForkResolutionSpec extends AnyFlatSpec with Matchers with IntegrationSuite with Eventually {

  val nodesQty: Int = 4

  // sync v2 finds a common point only through the headers 0, 16, 128 and 512 blocks
  // below a node's tip (`ErgoHistoryReader.FullV2SyncOffsets`). An isolated follower can
  // mine 20 blocks or more, past the 16 offset, so the common chain is at least 129
  // blocks long: then the header 128 below a forked tip exists and is common
  val commonChainLength: Int = 129
  val forkLength: Int = 5
  val syncLength: Int = 15

  val localVolumes: Seq[String] = (1 to nodesQty).map(localVolume)
  val remoteVolume = "/app"

  val volumesMapping: Seq[(String, String)] = localVolumes.map(_ -> remoteVolume)

  val dirs: Seq[File] = localVolumes.map(vol => new File(vol))
  dirs.foreach(_.mkdirs())

  val miningTimingConfig: Config = shortInternalMinerPollingInterval
    .withFallback(blockIntervalConfig(500))

  val nodeConfigs: List[Config] = nodeSeedConfigs.take(4)
    .map(_.withFallback(allowLocalConfig).withFallback(miningTimingConfig))

  val minerConfig: Config = nodeConfigs.head

  // node01 mines the common chain alone, so it can poll fast, as in LongChainSyncSpec
  val commonChainMinerConfig: Config =
    internalMinerPollingIntervalConfig(100).withFallback(minerConfig)

  val onlineSyncNodesConfig: List[Config] = nodeConfigs.slice(1, nodesQty)
    .map(_.withFallback(nonGeneratingPeerConfig))
  val offlineMiningNodesConfig: List[Config] = nodeConfigs.slice(1, nodesQty)

  def localVolume(n: Int): String = s"$localDataDir/fork-resolution-spec/node-$n/data"

  def selectedHeadersScore(node: Node): Future[BigInt] = node.status.map { response =>
    withClue(s"${node.nodeName} /info headersScore: ") {
      val info = io.circe.parser.parse(response.status).toOption.value
      info.hcursor.downField("headersScore").focus.value.asNumber.flatMap(_.toBigInt).value
    }
  }

  def clearPeerDatabases(mapping: Seq[(String, String)] = volumesMapping): Unit = {
    mapping.foreach { case (localVolume, remoteVolume) =>
      docker.removeFromMountedVolume(localVolume, remoteVolume, "peers")
    }
  }

  def startNodesWithBinds(nodeConfigs: List[Config],
                          configEnrich: ExtraConfig = noExtraConfig,
                          mapping: Seq[(String, String)] = volumesMapping): List[Node] = {
    require(nodeConfigs.size == mapping.size, "each node needs its own data volume")
    log.trace(s"Starting ${nodeConfigs.size} containers")
    val nodes: Try[List[Node]] = nodeConfigs
      .map(_.withFallback(specialDataDirConfig(remoteVolume)))
      .zip(mapping)
      .map { case (cfg, vol) => docker.startDevNetNode(cfg, configEnrich, Some(vol)) }
      .sequence
    implicit val patienceConfig: PatienceConfig = PatienceConfig((nodeConfigs.size * 2).seconds, 3.second)
    eventually {
      Await.result(Future.traverse(nodes.get)(_.waitForStartup), 180.seconds)
    }
  }

  // Testing scenario:
  // 1. Start up {nodesQty} nodes and let node01 mine a common chain of
  //    {commonChainLength} blocks;
  // 2. Stop all nodes (a clean shutdown) when they are done, make them offline generating,
  //    clear their peer databases and restart them with no connections allowed;
  // 3. Let them mine offline until each has its own block {forkLength} above the followers'
  //    headers, so that every follower has a fork;
  // 4. Stop all nodes again and restart with `knownPeers` filled, wait another {syncLength} blocks;
  // 5. Check that nodes reached consensus on created forks;
  it should "Fork resolution after isolated mining" in {

    log.info(minerConfig.toString)
    onlineSyncNodesConfig.foreach(x => log.info(x.toString))

    val nodes: List[Node] =
      startNodesWithBinds(commonChainMinerConfig +: onlineSyncNodesConfig)

    val result = Async.async {
      val initMaxHeight = Async.await(Future.traverse(nodes)(_.fullHeight).map(_.max))
      Async.await(Future.traverse(nodes)(_.waitForHeight(initMaxHeight + commonChainLength, 100.millis)))
      // Isolated followers can't mine while their headers are 6+ blocks ahead of full blocks
      val followerInfos = Async.await(nodes.head.waitForProgress[Seq[NodeInfo], Int](
        "followers' headers less than 3 ahead of their full blocks",
        _ => Future.traverse(nodes.tail)(_.info),
        _.forall(i => i.bestHeaderHeightOpt.getOrElse(0) - i.bestBlockHeightOpt.getOrElse(0) < 3),
        100.millis
      )(
        // a follower whose block download stops keeps the minimum still
        infos => Some(infos.map(_.bestBlockHeightOpt.getOrElse(0)).min),
        infos => nodes.tail.zip(infos).map { case (n, i) =>
          s"${n.nodeName} headers=${i.bestHeaderHeightOpt.getOrElse(0)} full=${i.bestBlockHeightOpt.getOrElse(0)}"
        }.mkString(", ")
      ))
      // the followers know no header at forkHeight, so each of them mines its own block there
      val forkHeight = followerInfos.map(_.bestHeaderHeightOpt.getOrElse(0)).max + forkLength
      val isolatedNodes = Async.await {
        nodes.foreach(node => docker.stopNode(node.containerId))
        clearPeerDatabases()
        Future.successful(startNodesWithBinds(minerConfig +: offlineMiningNodesConfig, noConnectionsConfig))
      }
      Async.await(Future.traverse(isolatedNodes)(_.waitForHeight(forkHeight, 100.millis)))
      val isolatedPeers = Async.await(Future.traverse(isolatedNodes) { node =>
        node.connectedPeers.map(peers => node.nodeName -> peers.map(_.name))
      })
      isolatedPeers.filter(_._2.nonEmpty) shouldBe empty
      // no follower has node01's block at forkHeight, so every follower has a fork to resolve
      val isolatedIds =
        Async.await(Future.traverse(isolatedNodes)(_.headerIdsByHeight(forkHeight).map(_.headOption)))
      isolatedIds.tail should not contain isolatedIds.head
      val regularNodes = Async.await {
        isolatedNodes.foreach(node => docker.stopNode(node.containerId))
        clearPeerDatabases()
        Future.successful(startNodesWithBinds(minerConfig +: onlineSyncNodesConfig))
      }
      Async.await(Future.traverse(regularNodes)(_.waitForHeight(forkHeight + syncLength, 100.millis)))
      val sample = Async.await(regularNodes.head.headerIdsByHeight(forkHeight)).headOption.value
      val headers = Async.await(Future.traverse(regularNodes) { node =>
        node.waitForProgress[Seq[String], String](
          s"header $sample at height $forkHeight",
          _.headerIdsByHeight(forkHeight),
          _.headOption.contains(sample),
          100.millis
        )(_.headOption, ids => s"selected ${ids.headOption.getOrElse("none")}")
      })

      log.debug(s"Headers at height $forkHeight: ${headers.mkString(",")}")
      val headerIdsAtSameHeight = headers.map(_.headOption.value)
      headerIdsAtSameHeight should contain only sample
    }

    Await.result(result, 15.minutes)
  }

  it should "switch a V2 fork with no selected-chain header in its locator" in {
    // The earlier case leaves its final nodes running until the suite closes.
    docker.nodes.foreach(node => docker.stopNode(node.containerId))
    val oracleVolumes = (1 to 2).map(n => s"$localDataDir/v2-no-common-locator/node-$n/data")
    oracleVolumes.foreach(v => new File(v).mkdirs())
    val oracleMapping = oracleVolumes.map(_ -> remoteVolume)
    val nonGeneratingConfigs = nodeConfigs.take(2).map(nonGeneratingPeerConfig.withFallback)
    val online = startNodesWithBinds(
      List(commonChainMinerConfig, nonGeneratingConfigs(1)), mapping = oracleMapping)

    val result = Async.async {
      val initialHeight = Async.await(Future.traverse(online)(_.fullHeight).map(_.max))
      val commonTarget = initialHeight + 16
      Async.await(online.head.waitForHeight(commonTarget, 100.millis))
      // Keep the producer available for sections while its follower catches up,
      // without letting the common tip advance between the check and isolation.
      val quiescedLeader = Async.await {
        docker.stopNode(online.head.containerId)
        Future.successful(startNodesWithBinds(
          List(nonGeneratingConfigs.head), mapping = oracleMapping.take(1)).head)
      }
      Async.await(quiescedLeader.waitForPeers(1))
      Async.await(online(1).waitForPeers(1))
      val commonLeaderInfo = Async.await(quiescedLeader.info)
      commonLeaderInfo.bestBlockIdOpt should not be empty
      Async.await(online(1).waitForProgress[NodeInfo, String](
        "follower caught up to the quiesced common producer",
        _.info,
        info => info.bestHeaderIdOpt == commonLeaderInfo.bestHeaderIdOpt &&
          info.bestBlockIdOpt == commonLeaderInfo.bestBlockIdOpt &&
          info.bestHeaderHeightOpt.getOrElse(0) - info.bestBlockHeightOpt.getOrElse(0) < 3,
        100.millis
      )(_.bestBlockIdOpt, info =>
        s"headers=${info.bestHeaderHeightOpt.getOrElse(0)} " +
          s"full=${info.bestBlockHeightOpt.getOrElse(0)}"))

      val isolated = Async.await {
        docker.stopNode(quiescedLeader.containerId)
        docker.stopNode(online(1).containerId)
        clearPeerDatabases(oracleMapping)
        Future.successful(startNodesWithBinds(
          List(minerConfig, nodeConfigs(1)), noConnectionsConfig, oracleMapping))
      }
      val followerStart = Async.await(isolated(1).info).bestHeaderHeightOpt.value
      Async.await(isolated(1).waitForHeight(followerStart + 24, 100.millis))
      val followerOfflineTip = Async.await(isolated(1).info).bestHeaderHeightOpt.value
      docker.stopNode(isolated(1).containerId)
      Async.await(isolated.head.waitForHeight(followerOfflineTip + 5, 100.millis))

      val frozen = Async.await {
        docker.stopNode(isolated.head.containerId)
        clearPeerDatabases(oracleMapping)
        Future.successful(startNodesWithBinds(
          nonGeneratingConfigs, noConnectionsConfig, oracleMapping))
      }
      Async.await(Future.traverse(frozen)(_.connectedPeers)).flatten shouldBe empty
      val leaderInfo = Async.await(frozen.head.info)
      val followerInfo = Async.await(frozen(1).info)
      val leaderTip = leaderInfo.bestHeaderHeightOpt.value
      val followerTip = followerInfo.bestHeaderHeightOpt.value
      withClue("responding node must classify the forked peer as Younger: ") {
        leaderTip should be > followerTip
      }
      val leaderScore = Async.await(selectedHeadersScore(frozen.head))
      val followerScore = Async.await(selectedHeadersScore(frozen(1)))
      withClue("responding selected header chain must be heavier: ") {
        leaderScore should be > followerScore
      }
      val locatorHeights = Seq(0, 16, 128, 512)
        .map(followerTip - _).filter(_ >= 1)
      locatorHeights.size should be >= 2
      locatorHeights.last should be <= 400
      val followerRows = Async.await(Future.traverse(locatorHeights)(frozen(1).headerIdsByHeight))
      val leaderRows = Async.await(Future.traverse(locatorHeights)(frozen.head.headerIdsByHeight))
      locatorHeights.zip(followerRows).foreach { case (height, ids) =>
        withClue(s"forked peer header at $height: ") { ids should not be empty }
      }
      locatorHeights.zip(leaderRows).foreach { case (height, ids) =>
        withClue(s"responding peer header at $height: ") { ids should not be empty }
      }
      val followerLocator = followerRows.map(_.head)
      val leaderSelected = leaderRows.map(_.head)
      locatorHeights.indices.foreach { i =>
        withClue(s"V2 locator at height ${locatorHeights(i)}: ") {
          followerLocator(i) should not be (leaderSelected(i))
        }
      }
      Async.await(frozen.head.headerIdsByHeight(1)).headOption.value shouldBe
        Async.await(frozen(1).headerIdsByHeight(1)).headOption.value
      val selectedHeight = locatorHeights.last
      val selectedId = leaderSelected.last
      val leaderBestId = leaderInfo.bestHeaderIdOpt.value

      val reconnected = Async.await {
        frozen.foreach(node => docker.stopNode(node.containerId))
        clearPeerDatabases(oracleMapping)
        Future.successful(startNodesWithBinds(
          nonGeneratingConfigs, noExtraConfig, oracleMapping))
      }
      val leaderPeers = Async.await(reconnected.head.waitForPeers(1)).map(_.name)
      val followerPeers = Async.await(reconnected(1).waitForPeers(1)).map(_.name)
      leaderPeers should contain only (reconnected(1).nodeName)
      followerPeers should contain only (reconnected.head.nodeName)
      // The frozen locator and two non-generating peers bound the possible sync paths.
      // An old-product run on this same fixture must still fail before attributing
      // convergence to the new from-genesis V2 response.
      Async.await(reconnected(1).waitForProgress[NodeInfo, String](
        "forked peer's selected tip after V2 continuation",
        _.info,
        _.bestHeaderIdOpt.contains(leaderBestId),
        100.millis
      )(_.bestHeaderIdOpt, info => s"selected ${info.bestHeaderIdOpt.getOrElse("none")}"))
      val followerSelectedAfter = Async.await(reconnected(1).headerIdsByHeight(selectedHeight))
      val leaderSelectedAfter = Async.await(reconnected.head.headerIdsByHeight(selectedHeight))
      followerSelectedAfter.headOption.value shouldBe selectedId
      leaderSelectedAfter.headOption.value shouldBe selectedId
    }

    Await.result(result, 15.minutes)
  }

}
