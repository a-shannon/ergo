package org.ergoplatform.it.util

import io.circe.Json

import java.util.concurrent.TimeoutException
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/** Bounded, allowlisted observations collected only after an initial-seed timeout. */
object DeepRollbackFailureDiagnostics {
  sealed trait Kind { def label: String }
  case object Info extends Kind { val label = "info" }
  case object Connected extends Kind { val label = "connected" }
  case object Sync extends Kind { val label = "sync" }
  case object Delivery extends Kind { val label = "delivery" }
  case object FullBlock extends Kind { val label = "fullBlock" }
  case class Request(node: Int, kind: Kind, run: () => Future[Json])

  private val invalid = Json.fromString("invalid")
  private val states = Set("Unknown", "Older", "Younger", "Equal", "Fork", "Nonsense")
  def validId(id: String): Boolean = id.matches("[0-9a-fA-F]{64}")

  private def number(value: Json): Option[Json] =
    value.asNumber.flatMap(_.toBigInt).filter(_ >= 0).map(Json.fromBigInt)

  private def field(body: Json, name: String)(decode: Json => Option[Json]): (String, Json) =
    name -> body.hcursor.downField(name).focus.filterNot(_.isNull)
      .map(value => decode(value).getOrElse(invalid)).getOrElse(Json.Null)

  private def deliveries(body: Json, name: String): Json = {
    body.hcursor.downField(name).focus.filterNot(_.isNull).map { value =>
      value.asObject.map { groups =>
        val safeGroups = groups.toVector.flatMap { case (key, entries) =>
          // Modifier types are bytes; never copy arbitrary object keys or modifier IDs.
          scala.util.Try(key.toInt).toOption.filter(n => n >= 0 && n <= 255).map { modifierType =>
            val records = entries.asObject.map(_.values.toVector)
            val checks = records.toSeq.flatten.flatMap(_.hcursor.get[Int]("checks").toOption).filter(_ >= 0)
            val fields = Seq("type" -> Json.fromInt(modifierType),
              "count" -> records.map(v => Json.fromInt(v.size)).getOrElse(invalid))
            val retries = if (name == "requested") Seq(
              "minChecks" -> checks.reduceOption(_ min _).map(Json.fromInt).getOrElse(Json.Null),
              "maxChecks" -> checks.reduceOption(_ max _).map(Json.fromInt).getOrElse(Json.Null)) else Seq.empty
            Json.obj((fields ++ retries): _*)
          }
        }.sortBy(_.hcursor.get[Int]("type").getOrElse(0))
        Json.arr(safeGroups: _*)
      }.getOrElse(invalid)
    }.getOrElse(Json.Null)
  }

  def project(node: Int, kind: Kind, sampledAt: Long, response: Either[String, Json]): Json = {
    val identity = Seq("node" -> Json.fromInt(node), "kind" -> Json.fromString(kind.label),
      "sampledAt" -> Json.fromLong(sampledAt))
    val payload = response match {
      case Left(_) => Json.obj("result" -> Json.fromString("unavailable"))
      case Right(body) => kind match {
        case Info if body.isObject =>
          val heights = Seq("headersHeight", "fullHeight").map(field(body, _)(number))
          val ids = Seq("bestHeaderId", "bestFullHeaderId", "genesisBlockId").map { name =>
            field(body, name)(_.asString.filter(validId).map(Json.fromString))
          }
          Json.obj((heights ++ ids :+ field(body, "isMining")(_.asBoolean.map(Json.fromBoolean))): _*)
        case Connected =>
          Json.obj("count" -> body.asArray.map(v => Json.fromInt(v.size)).getOrElse(invalid))
        case Sync =>
          Json.obj("peers" -> body.asArray.map { peers =>
            Json.arr(peers.map { peer => Json.obj(
              field(peer, "status")(_.asString.filter(states).map(Json.fromString)),
              field(peer, "height")(number))
            }: _*)
          }.getOrElse(invalid))
        case Delivery if body.isObject =>
          Json.obj("requested" -> deliveries(body, "requested"), "received" -> deliveries(body, "received"))
        case FullBlock if body.isObject =>
          Json.obj(field(body, "status")(_.asNumber.flatMap(_.toInt).filter(n => n >= 100 && n <= 599).map(Json.fromInt)),
            field(body, "present")(_.asBoolean.map(Json.fromBoolean)))
        case _ => Json.obj("result" -> invalid)
      }
    }
    Json.obj((identity ++ payload.asObject.toSeq.flatMap(_.toVector)): _*)
  }

  def rethrowAfterCapture(original: TimeoutException, requests: => Seq[Request])
                         (emit: String => Unit)(implicit ec: ExecutionContext): Unit = {
    try {
      val observations = new ConvergenceObservations
      try {
        val snapshots = requests.map { request =>
          observations.probe(Future(request.run()).flatMap(identity)).sample(2.seconds)
            .map(result => project(request.node, request.kind, System.currentTimeMillis(), result))
        }
        // Separate post-failure budget; the original assertion has already failed.
        emit(Json.arr(Await.result(Future.sequence(snapshots), 3.seconds): _*).noSpaces)
      } finally observations.close()
    } finally throw original
  }
}
