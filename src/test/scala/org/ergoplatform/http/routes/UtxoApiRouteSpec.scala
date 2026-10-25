package org.ergoplatform.http.routes

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import io.circe.syntax._
import org.ergoplatform.ErgoBox.BoxId
import org.ergoplatform.http.api.{ApiCodecs, UtxoApiRoute}
import org.ergoplatform.modifiers.mempool.{ErgoTransaction, UnconfirmedTransaction}
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.mempool.ErgoMemPoolReader
import org.ergoplatform.nodeView.mempool.OrderedTxPool.WeightedTxId
import org.ergoplatform.utils.Stubs
import org.ergoplatform.wallet.boxes.ErgoBoxSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.crypto.hash.Blake2b256
import scorex.util.ModifierId
import scorex.util.encode.Base16

import java.util.concurrent.atomic.AtomicInteger

class UtxoApiRouteSpec
  extends AnyFlatSpec
  with Matchers
  with ScalatestRouteTest
  with Stubs
  with FailFastCirceSupport
  with ApiCodecs {

  val prefix = "/utxo"
  val maxBatchItems = 16384

  val route: Route =
    UtxoApiRoute(utxoReadersRef, utxoSettings.scorexSettings.restApi).route
  val sealedRoute: Route = Route.seal(route)

  it should "get utxo box with /byId" in {
    val box   = utxoState.takeBoxes(1).head
    val boxId = Base16.encode(box.id)
    Get(prefix + s"/byId/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Json].hcursor.downField("value").as[Long] shouldEqual Right(box.value)
      responseAs[Json].hcursor.downField("boxId").as[String] shouldEqual Right(boxId)
    }
  }

  it should "get mempool box with withPool/byId" in {
    val box   = memPool.getAll.map(utx => utx.transaction).flatMap(_.outputs).head
    val boxId = Base16.encode(box.id)
    Get(prefix + s"/byId/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.NotFound
    }
    Get(prefix + s"/withPool/byId/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Json].hcursor.downField("value").as[Long] shouldEqual Right(box.value)
      responseAs[Json].hcursor.downField("boxId").as[String] shouldEqual Right(boxId)
    }
  }

  it should "get all mempool boxes with withPool/byIds" in {
    val boxes = memPool.getAll.map(utx => utx.transaction).flatMap(_.outputs)
    val boxesEncoded = boxes.map(box => Base16.encode(box.id))

    Post(prefix + "/withPool/byIds", boxesEncoded.asJson) ~> route ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Seq[Json]]
        .map(_.hcursor.downField("value").as[Long]) shouldEqual boxes.map(x => Right(x.value))
      responseAs[Seq[Json]]
        .map(_.hcursor.downField("boxId").as[String]) shouldEqual boxesEncoded.map(x => Right(x))
    }
  }

  it should "read the mempool once for a batch of box ids" in {
    val poolBoxId = Base16.encode(memPool.getAll.head.transaction.outputs.head.id)
    val stateBoxId = Base16.encode(utxoState.takeBoxes(1).head.id)
    val requestedIds = Seq(poolBoxId, stateBoxId)
    requestedIds.distinct.size shouldBe 2
    val snapshots = new AtomicInteger(0)
    val countingPool = new ErgoMemPoolReader {
      override def contains(id: ModifierId): Boolean = memPool.contains(id)
      override def getAll(ids: Seq[ModifierId]): Seq[UnconfirmedTransaction] = memPool.getAll(ids)
      override def size: Int = memPool.size
      override def spentInputs: Iterator[BoxId] = memPool.spentInputs
      override def getAll: Seq[UnconfirmedTransaction] = {
        snapshots.incrementAndGet()
        memPool.getAll
      }
      override def getAllPrioritized: Seq[UnconfirmedTransaction] = memPool.getAllPrioritized
      override def take(limit: Int): Iterable[UnconfirmedTransaction] = memPool.take(limit)
      override def random(limit: Int): Iterable[UnconfirmedTransaction] = memPool.random(limit)
      override def modifierById(id: ModifierId): Option[ErgoTransaction] = memPool.modifierById(id)
      override def unconfirmedById(id: ModifierId): Option[UnconfirmedTransaction] =
        memPool.unconfirmedById(id)
      override def weightedTransactionIds(limit: Int): Seq[WeightedTxId] =
        memPool.weightedTransactionIds(limit)
      override def getExpectedWaitTime(txFee: Long, txSize: Int): Long =
        memPool.getExpectedWaitTime(txFee, txSize)
      override def getRecommendedFee(expectedWaitTimeMinutes: Int, txSize: Int): Long =
        memPool.getRecommendedFee(expectedWaitTimeMinutes, txSize)
    }
    val countedReaders = Readers(history, utxoState, countingPool, wallet)
    val countedReadersRef = system.actorOf(Props(new Actor {
      def receive: Receive = { case GetReaders => sender() ! countedReaders }
    }))
    val countedRoute = UtxoApiRoute(countedReadersRef, utxoSettings.scorexSettings.restApi).route

    Post(prefix + "/withPool/byIds", requestedIds.asJson) ~> countedRoute ~> check {
      status shouldBe StatusCodes.OK
      val returnedIds = responseAs[Seq[Json]]
        .map(_.hcursor.downField("boxId").as[String])
      returnedIds shouldEqual requestedIds.map(Right(_))
    }
    snapshots.get() shouldBe 1

    Post(prefix + "/withPool/byIds", Seq.empty[String].asJson) ~> countedRoute ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Seq[Json]] shouldBe empty
    }
    snapshots.get() shouldBe 1
  }

  it should "not found utxo box with /byId" in {
    val boxId = Base16.encode(Blake2b256(utxoState.takeBoxes(1).head.id))
    Get(prefix + s"/byId/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.NotFound
    }
  }

  it should "reject invalid box id instead of failing the route" in {
    Get(prefix + s"/byId/not-hex") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Get(prefix + s"/byId/00") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Get(prefix + s"/byIdBinary/not-hex") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Get(prefix + s"/byIdBinary/00") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Get(prefix + s"/withPool/byId/not-hex") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Get(prefix + s"/withPool/byId/00") ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Post(prefix + "/withPool/byIds", Seq("not-hex").asJson) ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
    Post(prefix + "/withPool/byIds", Seq("00").asJson) ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
  }

  it should "reject oversized UTXO batch requests" in {
    val boxId = Base16.encode(Blake2b256(utxoState.takeBoxes(1).head.id))
    val tooManyIds = Seq.fill(maxBatchItems + 1)(boxId)

    Post(prefix + "/withPool/byIds", tooManyIds.asJson) ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }

    Post(prefix + s"/getBoxesBinaryProof", tooManyIds.asJson) ~> sealedRoute ~> check {
      status shouldBe StatusCodes.BadRequest
    }
  }

  it should "get utxo box with /byIdBinary" in {
    val box   = utxoState.takeBoxes(1).head
    val boxId = Base16.encode(box.id)
    Get(prefix + s"/byIdBinary/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Json].hcursor.downField("boxId").as[String] shouldEqual Right(boxId)
      val bytes = Base16
        .decode(responseAs[Json].hcursor.downField("bytes").as[String].toOption.get)
        .get
      val boxRestored = ErgoBoxSerializer.parseBytes(bytes)
      box shouldEqual boxRestored
    }
  }

  it should "not found utxo box with /byIdBinary" in {
    val boxId = Base16.encode(Blake2b256(utxoState.takeBoxes(1).head.id))
    Get(prefix + s"/byId/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.NotFound
    }
  }

  it should "get pool box with /withPool/byIdBinary" in {
    val box   = memPool.getAll.map(utx => utx.transaction).flatMap(_.outputs).head
    val boxId = Base16.encode(box.id)
    Get(prefix + s"/byIdBinary/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.NotFound
    }
    Get(prefix + s"/withPool/byIdBinary/$boxId") ~> route ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Json].hcursor.downField("boxId").as[String] shouldEqual Right(boxId)
      val bytes = Base16
        .decode(responseAs[Json].hcursor.downField("bytes").as[String].toOption.get)
        .get
      val boxRestored = ErgoBoxSerializer.parseBytes(bytes)
      box shouldEqual boxRestored
    }
  }

  it should "/genesis returns 3 boxes" in {
    Get(prefix + s"/genesis") ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[List[Json]]
      response.size shouldBe 3 // 3 genesis boxes as per Ergo Whitepaper
    }
  }

  it should "get serialized proof for given boxes" in {
    val boxes = utxoState.takeBoxes(10).map(box => Base16.encode(box.id))
    Post(prefix + s"/getBoxesBinaryProof", boxes.asJson) ~> route ~> check {
      status shouldBe StatusCodes.OK
    }
  }
}
