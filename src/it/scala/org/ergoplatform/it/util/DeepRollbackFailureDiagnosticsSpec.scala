package org.ergoplatform.it.util

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class DeepRollbackFailureDiagnosticsSpec extends AnyFlatSpec with Matchers {
  import DeepRollbackFailureDiagnostics._
  implicit private val ec: ExecutionContext = ExecutionContext.global
  private val marker = "private-address-name-and-transaction"
  private val id = "ab" * 32
  private def json(value: String): Json = parse(value).toOption.get
  private def projected(kind: Kind, value: Json): Json = project(0, kind, 1L, Right(value))

  "Seed failure diagnostics" should "project only typed info, peer states and block availability" in {
    val info = projected(Info, Json.obj("headersHeight" -> Json.fromInt(32), "fullHeight" -> Json.fromInt(18),
      "bestHeaderId" -> Json.fromString(id), "isMining" -> Json.False, "name" -> Json.fromString(marker)))
    info.hcursor.get[Int]("headersHeight") shouldBe Right(32)
    info.hcursor.get[Int]("fullHeight") shouldBe Right(18)
    info.hcursor.get[String]("bestHeaderId") shouldBe Right(id)
    info.hcursor.get[Boolean]("isMining") shouldBe Right(false)
    info.hcursor.downField("genesisBlockId").focus shouldBe Some(Json.Null)
    val connected = projected(Connected, Json.arr(Json.fromString(marker), Json.obj("name" -> Json.fromString(marker))))
    connected.hcursor.get[Int]("count") shouldBe Right(2)
    val sync = projected(Sync, Json.arr(Json.obj("status" -> Json.fromString("Equal"),
      "height" -> Json.fromInt(32), "address" -> Json.fromString(marker))))
    sync.hcursor.downField("peers").downArray.get[String]("status") shouldBe Right("Equal")
    sync.hcursor.downField("peers").downArray.get[Int]("height") shouldBe Right(32)
    val full = projected(FullBlock, Json.obj("status" -> Json.fromInt(404), "present" -> Json.False,
      "transactions" -> Json.fromString(marker)))
    full.hcursor.get[Int]("status") shouldBe Right(404)
    full.hcursor.get[Boolean]("present") shouldBe Right(false)
    Seq(info, connected, sync, full).foreach(_.noSpaces should not include marker)
  }

  it should "aggregate delivery counts and retries without copying keys or peer contents" in {
    val body = json(s"""{"requested":{"102":{"$marker":{"checks":7,"address":"$marker"},"$id":{"checks":2}},"$marker":{"checks":99},"999":{}},"received":{"104":{"$marker":{"address":"$marker"}}},"extra":"$marker"}""")
    val result = projected(Delivery, body)
    val request = result.hcursor.downField("requested").downArray
    request.get[Int]("type") shouldBe Right(102)
    request.get[Int]("count") shouldBe Right(2)
    request.get[Int]("minChecks") shouldBe Right(2)
    request.get[Int]("maxChecks") shouldBe Right(7)
    result.hcursor.downField("received").downArray.get[Int]("count") shouldBe Right(1)
    result.hcursor.downField("received").downArray.get[Int]("type") shouldBe Right(104)
    result.noSpaces should not include marker
    result.noSpaces should not include id
  }

  it should "mark malformed and missing fields without leaking unexpected JSON" in {
    val bad = Json.obj("headersHeight" -> Json.fromString(marker), "fullHeight" -> Json.fromInt(-1),
      "bestHeaderId" -> Json.fromString(marker), "isMining" -> Json.obj("secret" -> Json.fromString(marker)))
    val info = projected(Info, bad)
    Seq("headersHeight", "fullHeight", "bestHeaderId", "isMining").foreach { key =>
      info.hcursor.get[String](key) shouldBe Right("invalid")
    }
    projected(Sync, Json.arr(Json.obj("status" -> Json.fromString(marker), "height" -> Json.Null)))
      .hcursor.downField("peers").downArray.get[String]("status") shouldBe Right("invalid")
    projected(Delivery, json(s"""{"requested":{"102":{"$marker":{"checks":"$marker"}}},"received":"$marker"}"""))
      .hcursor.downField("requested").downArray.downField("minChecks").focus shouldBe Some(Json.Null)
    Seq(Info, Connected, Sync, Delivery, FullBlock).foreach { kind =>
      Seq(Json.Null, Json.fromString(marker), bad).foreach { body =>
        projected(kind, body).noSpaces should not include marker
      }
      project(1, kind, 2L, Left(marker)).noSpaces should not include marker
    }
  }

  it should "preserve the original failure through transport, request creation and emission failures" in {
    val original = new TimeoutException("original")
    var captured = ""
    val requests = Seq(Request(0, Info, () => Future.failed(new IOException(marker))),
      Request(1, Delivery, () => throw new IllegalStateException(marker)))
    val transport = intercept[TimeoutException] {
      rethrowAfterCapture(original, requests)(text => captured = text)
    }
    (transport eq original) shouldBe true
    captured should not include marker
    json(captured).asArray.get.size shouldBe 2
    val creation = intercept[TimeoutException] {
      rethrowAfterCapture(original, throw new IllegalArgumentException(marker))(_ => ())
    }
    (creation eq original) shouldBe true
    val emission = intercept[TimeoutException] {
      rethrowAfterCapture(original, Seq.empty)(_ => throw new IllegalArgumentException(marker))
    }
    (emission eq original) shouldBe true
  }

  it should "start ten hanging requests concurrently and retain the original failure within a separate bound" in {
    val original = new TimeoutException("original")
    val started = new AtomicInteger(0)
    val requests = Seq.fill(10)(Promise[Json]()).zipWithIndex.map { case (pending, index) =>
      Request(index % 2, Info, () => { started.incrementAndGet(); pending.future })
    }
    var captured = ""
    val thrown = intercept[TimeoutException] {
      Await.result(Future { rethrowAfterCapture(original, requests)(text => captured = text) }, 5.seconds)
    }
    (thrown eq original) shouldBe true
    started.get() shouldBe 10
    val results = json(captured).asArray.get
    results.size shouldBe 10
    results.foreach(_.hcursor.get[String]("result") shouldBe Right("unavailable"))
  }
}
