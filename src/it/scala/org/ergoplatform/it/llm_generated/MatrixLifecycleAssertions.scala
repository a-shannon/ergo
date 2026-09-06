package org.ergoplatform.it

import io.circe.{Decoder, Json}
import org.ergoplatform.ErgoBox
import org.ergoplatform.http.api.ApiCodecs
import org.ergoplatform.it.container.Node
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.scalatest.matchers.should.Matchers
import scorex.util.encode.Base16

/** Shared observations for ordinary Matrix lifecycle tests. */
private[it] trait MatrixLifecycleAssertions extends Matchers with ApiCodecs {
  protected def get(node: Node, path: String): Json
  protected def field[A: Decoder](json: Json, name: String): A
  protected def until[A](label: String)(observe: => A)(accept: A => Boolean): A
  protected def height(node: Node): Int
  protected def inputTip(node: Node): String
  protected def mempool(node: Node): Set[String]
  protected def walletIds(node: Node): Set[String]
  protected def walletBalance(node: Node): Long

  protected def assertConfirmed(orderingId: String, txs: Seq[ErgoTransaction], nodes: Seq[Node]): Unit = {
    nodes.foreach { node =>
      val section = get(node, s"/blocks/$orderingId/transactions")
      field[String](section, "headerId") shouldBe orderingId
      val ids = field[Vector[Json]](section, "transactions").map(field[String](_, "id"))
      txs.foreach { tx =>
        ids.count(_ == tx.id) shouldBe 1
        val outputId = Base16.encode(tx.outputs.head.id)
        val confirmed = get(node, s"/utxo/byId/$outputId").as[ErgoBox].fold(throw _, identity)
        Base16.encode(confirmed.id) shouldBe outputId
      }
    }
  }

  protected def assertInputBody(node: Node, id: String, transactions: Vector[ErgoTransaction]): Unit = {
    until(s"$id has exact transaction IDs and bodies") {
      (get(node, s"/blocks/$id/inputBlockTransactionIds").as[Option[Vector[String]]].fold(throw _, identity),
        get(node, s"/blocks/$id/inputBlockTransactions").as[Option[Vector[ErgoTransaction]]].fold(throw _, identity)
          .map(_.map(_.bytes.toVector)))
    }(_ == ((Some(transactions.map(_.id)), Some(transactions.map(_.bytes.toVector)))))
  }

  protected def assertOfflineCheckpoint(node: Node, checkpoint: (String, String), confirmedHeight: Int,
                                        boxes: Set[String], balance: Long): Unit = {
    get(node, "/peers/connected").asArray.get shouldBe empty
    until("confirmed checkpoint restored locally") {
      val info = get(node, "/info")
      (field[Option[String]](info, "bestFullHeaderId").getOrElse(""),
        field[Option[String]](info, "stateRoot").getOrElse(""))
    }(_ == checkpoint)
    until("wallet checkpoint restored locally")(field[Int](get(node, "/wallet/status"), "walletHeight"))(_ == confirmedHeight)
    height(node) shouldBe confirmedHeight
    walletIds(node) shouldBe boxes
    walletBalance(node) shouldBe balance
    mempool(node) shouldBe empty
    inputTip(node) shouldBe empty
    field[Vector[String]](get(node, "/blocks/bestInputChain"), "bestInputBlocks") shouldBe empty
    get(node, "/peers/connected").asArray.get shouldBe empty
  }

  protected def assertFrozenInputChain(nodes: Seq[Node], tip: String, chain: Json,
                                       bodies: Map[String, Vector[ErgoTransaction]], boxes: Set[String],
                                       balance: Long, confirmedHeight: Int): Unit = {
    nodes.foreach { node =>
      until("frozen input tip converges")(inputTip(node))(_ == tip)
      until("frozen input ancestry is fully processed")(get(node, "/blocks/bestInputChain"))(_ == chain)
      bodies.foreach { case (id, transactions) => assertInputBody(node, id, transactions) }
      until("frozen input wallet converges")(walletIds(node))(_ == boxes)
      walletBalance(node) shouldBe balance
      until("frozen input mempool converges")(mempool(node))(_.isEmpty)
      height(node) shouldBe confirmedHeight
    }
  }
}
