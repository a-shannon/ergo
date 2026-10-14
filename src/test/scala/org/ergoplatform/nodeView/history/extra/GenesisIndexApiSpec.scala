package org.ergoplatform.nodeView.history.extra

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import ch.qos.logback.classic.{Logger => LogbackLogger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.ergoplatform.http.api.BlockchainApiRoute
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.GetDataFromCurrentView
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef}
import org.ergoplatform.nodeView.history.ErgoHistory
import org.ergoplatform.nodeView.state.{ErgoState, ErgoStateReader, StateType}
import org.ergoplatform.settings.Algos
import org.ergoplatform.utils.Stubs
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import scorex.util.bytesToId

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._

class GenesisIndexApiSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest
  with FailFastCirceSupport with Stubs {
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  private class StartupLogAppender extends AppenderBase[ILoggingEvent] {
    val messages = new ConcurrentLinkedQueue[String]()
    override protected def append(event: ILoggingEvent): Unit = messages.add(event.getFormattedMessage)
  }

  it should "serve positive genesis references and bounded ranges without exposing reserved zero" in {
    GenesisIndex.initialize(history.historyStorage, settings.chainSettings, rentEnabled = false,
      modeProvenanceBytes = ExtraIndexer.UtxoIndexProvenanceBytes).get
    val route = akka.http.scaladsl.server.Route.seal(BlockchainApiRoute(digestReadersRef,
      settings.copy(nodeSettings = settings.nodeSettings.copy(extraIndex = true)), None).route)
    val expected = org.ergoplatform.nodeView.state.ErgoState.genesisBoxes(settings.chainSettings).map(box => bytesToId(box.id))
    Seq(0L, -1L, 4L, Long.MaxValue).foreach { number =>
      Get(s"/blockchain/box/byIndex/$number") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
    expected.zipWithIndex.foreach { case (id, index) =>
      Get(s"/blockchain/box/byIndex/${index + 1}") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[io.circe.Json].hcursor.get[String]("boxId").toOption.get shouldBe id
      }
    }
    Seq((0, 5, expected.reverse), (2, 5, expected.take(1)), (3, 5, Seq.empty),
      (Int.MaxValue, 5, Seq.empty), (0, 0, Seq.empty)).foreach { case (offset, limit, ids) =>
      Get(s"/blockchain/box/range?offset=$offset&limit=$limit") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[Seq[String]] shouldBe ids
      }
    }
    Seq("offset=-1", "limit=-1").foreach { invalid =>
      Get(s"/blockchain/box/range?$invalid") ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
  }

  it should "retain zero as transient metadata for unconfirmed boxes" in {
    val tree = memPool.getAll.head.transaction.outputs.head.ergoTree
    val address = IndexedErgoAddress(IndexedErgoAddressSerializer.hashErgoTree(tree))
    val expected = memPool.getAll.flatMap(_.transaction.outputs).filter(_.ergoTree == tree).map(box => bytesToId(box.id))
    expected should not be empty
    val returned = address.retrieveUtxos(history, memPool, 0, 10000,
      org.ergoplatform.http.api.SortDirection.ASC, unconfirmed = true, Set.empty)
    returned.map(_.id).toSet shouldBe expected.toSet
    returned.forall(_.globalIndex == 0L) shouldBe true
    NumericBoxIndex.getBoxByNumber(history, 0L) shouldBe None
  }

  it should "serve atomic Digest genesis rows through a bound real holder and preserve them after reopen" in {
    val indexedSettings = settings.copy(directory = createTempDir.getAbsolutePath,
      nodeSettings = settings.nodeSettings.copy(stateType = StateType.Digest, extraIndex = true,
        verifyTransactions = true, storageRentCollection = false))
    val expected = ErgoState.genesisBoxes(indexedSettings.chainSettings).head
    val persistenceWitnessKey = Algos.hash("GenesisIndexApiSpec bound HTTP persistence witness")
    val persistenceWitnessValue = "preserved-after-bound-http".getBytes(StandardCharsets.UTF_8)
    val probe = TestProbe()(system)
    val holder = ErgoNodeViewRef(indexedSettings)
    probe.watch(holder)
    val startupLogger = LoggerFactory.getLogger(classOf[ExtraIndexer]).asInstanceOf[LogbackLogger]
    val startupAppender = new StartupLogAppender
    startupAppender.start()
    startupLogger.addAppender(startupAppender)
    var indexerOpt = Option.empty[ActorRef]
    var readersHolderOpt = Option.empty[ActorRef]
    var bindingOpt = Option.empty[ServerBinding]
    try {
      probe.send(holder, GetDataFromCurrentView[ErgoStateReader, (ErgoHistory, Boolean)] { view =>
        (view.history, view.state.isInstanceOf[org.ergoplatform.nodeView.state.DigestState])
      })
      val (genesisHistory, isDigestState) = probe.expectMsgType[(ErgoHistory, Boolean)](15.seconds)
      isDigestState shouldBe true
      val checkpoint = IndexerState.fromHistory(genesisHistory)
      checkpoint.indexedHeight shouldBe 0
      checkpoint.globalTxIndex shouldBe 0L
      checkpoint.globalBoxIndex shouldBe GenesisIndex.nextBoxIndex(indexedSettings.chainSettings)
      NumericBoxIndex.getBoxByNumber(genesisHistory, 0L) shouldBe None
      genesisHistory.historyStorage.modifierBytesById(bytesToId(ExtraIndexer.IndexModeProvenanceKey))
        .exists(_.sameElements(ExtraIndexer.DigestIndexProvenanceBytes)) shouldBe true
      GenesisIndex.initialRowsMatch(genesisHistory.historyStorage, indexedSettings.chainSettings,
        rentEnabled = false) shouldBe true

      val indexer = ExtraIndexer(indexedSettings.chainSettings, indexedSettings.cacheSettings, holder)(system, false)
      indexerOpt = Some(indexer)
      probe.watch(indexer)
      org.ergoplatform.utils.untilTimeout(15.seconds, 50.millis) {
        startupAppender.messages.contains(
          "Started extra indexer at height 0, applied tip is None") shouldBe true
      }

      val readersHolder = ErgoReadersHolderRef(holder)(system)
      readersHolderOpt = Some(readersHolder)
      probe.watch(readersHolder)
      probe.send(readersHolder, GetReaders)
      val readers = probe.expectMsgType[Readers](15.seconds)
      readers.h shouldBe genesisHistory.getReader
      readers.s.isInstanceOf[org.ergoplatform.nodeView.state.DigestState] shouldBe true

      val route = Route.seal(BlockchainApiRoute(readersHolder, indexedSettings, Some(indexer))(system).route)
      val binding = await(Http().newServerAt("127.0.0.1", 0).bind(route))
      bindingOpt = Some(binding)
      binding.localAddress.getAddress.isLoopbackAddress shouldBe true
      binding.localAddress.getPort should be > 0

      val response = await(Http().singleRequest(HttpRequest(
        uri = s"http://127.0.0.1:${binding.localAddress.getPort}/blockchain/box/byIndex/1")))
      response.status shouldBe StatusCodes.OK
      val body = await(response.entity.toStrict(5.seconds)).data.utf8String
      io.circe.parser.parse(body).flatMap(_.hcursor.get[String]("boxId")) shouldBe Right(bytesToId(expected.id))
      genesisHistory.historyStorage.insertExtraTry(
        Array(persistenceWitnessKey -> persistenceWitnessValue), Array.empty).get
      genesisHistory.historyStorage.modifierBytesById(bytesToId(persistenceWitnessKey))
        .exists(_.sameElements(persistenceWitnessValue)) shouldBe true
    } finally {
      try bindingOpt.foreach(binding => await(binding.unbind()))
      finally {
        try readersHolderOpt.foreach { readersHolder =>
          system.stop(readersHolder)
          probe.expectTerminated(readersHolder, 10.seconds)
        } finally {
          try indexerOpt.foreach { indexer =>
            system.stop(indexer)
            probe.expectTerminated(indexer, 10.seconds)
          } finally {
            try startupLogger.detachAppender(startupAppender)
            finally {
              try startupAppender.stop()
              finally {
                system.stop(holder)
                probe.expectTerminated(holder, 10.seconds)
              }
            }
          }
        }
      }
    }

    val reopened = ErgoHistory.readOrGenerate(indexedSettings)(null)
    try {
      reopened.historyStorage.modifierBytesById(bytesToId(ExtraIndexer.IndexModeProvenanceKey))
        .exists(_.sameElements(ExtraIndexer.DigestIndexProvenanceBytes)) shouldBe true
      GenesisIndex.initialRowsMatch(reopened.historyStorage, indexedSettings.chainSettings,
        rentEnabled = false) shouldBe true
      NumericBoxIndex.getBoxByNumber(reopened, 1L).map(_.id) shouldBe Some(bytesToId(expected.id))
      reopened.historyStorage.modifierBytesById(bytesToId(persistenceWitnessKey))
        .exists(_.sameElements(persistenceWitnessValue)) shouldBe true
      val reopenedCheckpoint = IndexerState.fromHistory(reopened)
      reopenedCheckpoint.indexedHeight shouldBe 0
      reopenedCheckpoint.globalTxIndex shouldBe 0L
      reopenedCheckpoint.globalBoxIndex shouldBe GenesisIndex.nextBoxIndex(indexedSettings.chainSettings)
    } finally reopened.closeStorage()
  }
}
