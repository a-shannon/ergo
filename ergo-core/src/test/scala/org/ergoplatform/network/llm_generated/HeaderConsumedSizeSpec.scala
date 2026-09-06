package org.ergoplatform.network.llm_generated

import org.ergoplatform.{ErgoBox, ErgoBoxCandidate, Input}
import org.ergoplatform.modifiers.ErgoFullBlock
import org.ergoplatform.modifiers.history.BlockTransactions
import org.ergoplatform.modifiers.history.extension.Extension
import org.ergoplatform.modifiers.history.header.{Header, HeaderSerializer}
import org.ergoplatform.modifiers.mempool.ErgoTransaction
import org.ergoplatform.network.message.inputblocks.{OrderingBlockAnnouncement, OrderingBlockAnnouncementMessageSpec}
import org.ergoplatform.settings.Algos
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.generators.ErgoCoreGenerators.defaultHeaderGen
import scorex.util.bytesToId
import scorex.util.serialization.VLQByteBufferReader
import sigma.Colls
import sigma.ast.ErgoTree
import sigma.data.TrivialProp.TrueProp
import sigma.interpreter.ProverResult

import java.nio.ByteBuffer

/** Codec accounting fixtures; no block or proof-of-work validation is performed. */
class HeaderConsumedSizeSpec extends ErgoCorePropertyTest {
  private def headerForVersion(header: Header, version: Byte): Header =
    header.copy(version = version, unparsedBytes = Array.emptyByteArray, sizeOpt = None)

  private def announcement(header: Header): OrderingBlockAnnouncement =
    OrderingBlockAnnouncement(OrderingBlockAnnouncement.CurrentVersion, header, Seq.empty, Seq.empty, Seq.empty)

  private def assertHeaderBytes(parsed: Header, original: Header, bytes: Array[Byte]): Unit = {
    HeaderSerializer.toBytes(parsed) shouldBe bytes
    parsed.bytes shouldBe bytes
    parsed.id shouldBe original.id
  }

  property("standalone V1 and V4 header sizes equal their consumed bytes at offset zero") {
    forAll(defaultHeaderGen) { generated =>
      Seq(1.toByte, 4.toByte).foreach { version =>
        val original = headerForVersion(generated, version)
        val bytes = HeaderSerializer.toBytes(original)
        val reader = new VLQByteBufferReader(ByteBuffer.wrap(bytes))
        reader.position shouldBe 0
        reader.consumed shouldBe 0
        val parsed = HeaderSerializer.parse(reader)
        reader.position shouldBe bytes.length
        reader.consumed shouldBe bytes.length
        assertHeaderBytes(parsed, original, bytes)
        parsed.sizeOpt shouldBe Some(bytes.length)
        parsed.size shouldBe bytes.length
        Header.jsonEncoder(parsed).hcursor.get[Int]("size") shouldBe Right(bytes.length)
      }
    }
  }

  property("a V4 header inside an ordinary announcement excludes the consumed version prefix from its size") {
    forAll(defaultHeaderGen) { generated =>
      val original = headerForVersion(generated, 4.toByte)
      val headerBytes = HeaderSerializer.toBytes(original)
      val bytes = OrderingBlockAnnouncementMessageSpec.toBytes(announcement(original))
      val reader = new VLQByteBufferReader(ByteBuffer.wrap(bytes))
      reader.getByte() shouldBe OrderingBlockAnnouncement.CurrentVersion
      reader.position shouldBe 1
      reader.consumed shouldBe 1
      val parsed = HeaderSerializer.parse(reader)
      reader.position shouldBe 1 + headerBytes.length
      reader.consumed shouldBe 1 + headerBytes.length
      // Continue on the same reader through the actual empty announcement sections.
      reader.getUInt() shouldBe 0L
      reader.getUInt() shouldBe 0L
      reader.getUShort() shouldBe 0
      reader.getUByte() shouldBe 0
      reader.position shouldBe bytes.length
      reader.consumed shouldBe bytes.length
      bytes.slice(1, 1 + headerBytes.length) shouldBe headerBytes
      assertHeaderBytes(parsed, original, headerBytes)
      parsed.sizeOpt shouldBe Some(headerBytes.length)
      parsed.size shouldBe headerBytes.length
    }
  }

  property("successive headers on a prefixed reader each cache only their own serialized size") {
    forAll(defaultHeaderGen) { generated =>
      val originals = Seq(1.toByte, 4.toByte).map(headerForVersion(generated, _))
      val serialized = originals.map(HeaderSerializer.toBytes)
      // Match the digest and header-count prefix used by ErgoStateContextSerializer.
      val prefix = Array.fill[Byte](33)(0) ++ Array(originals.size.toByte)
      val bytes = serialized.foldLeft(prefix)((acc, headerBytes) => acc ++ headerBytes)
      val reader = new VLQByteBufferReader(ByteBuffer.wrap(bytes))
      reader.getBytes(33) shouldBe prefix.take(33)
      reader.getUByte() shouldBe originals.size
      reader.position shouldBe prefix.length
      reader.consumed shouldBe prefix.length
      val parsed = originals.zip(serialized).map { case (original, headerBytes) =>
        val start = reader.position
        val recovered = HeaderSerializer.parse(reader)
        reader.position shouldBe start + headerBytes.length
        reader.consumed shouldBe reader.position
        assertHeaderBytes(recovered, original, headerBytes)
        recovered
      }
      reader.position shouldBe bytes.length
      parsed.map(_.sizeOpt) shouldBe serialized.map(b => Some(b.length))
      parsed.map(_.size) shouldBe serialized.map(_.length)
    }
  }

  property("official announcement parsing preserves bytes and reports header and full-block metadata sizes") {
    forAll(defaultHeaderGen) { generated =>
      val original = headerForVersion(generated, 4.toByte)
      val headerBytes = HeaderSerializer.toBytes(original)
      val bytes = OrderingBlockAnnouncementMessageSpec.toBytes(announcement(original))
      val reader = new VLQByteBufferReader(ByteBuffer.wrap(bytes))
      val recovered = OrderingBlockAnnouncementMessageSpec.parse(reader)
      reader.position shouldBe bytes.length
      reader.consumed shouldBe bytes.length
      recovered.version shouldBe OrderingBlockAnnouncement.CurrentVersion
      recovered.nonBroadcastedTransactions shouldBe empty
      recovered.broadcastedTransactionIds shouldBe empty
      recovered.extensionFields shouldBe empty
      recovered.unparsedBytes shouldBe empty
      OrderingBlockAnnouncementMessageSpec.toBytes(recovered) shouldBe bytes
      assertHeaderBytes(recovered.header, original, headerBytes)

      // BlockTransactions requires a nonempty collection: use one value-preserving True-box payment.
      val source = new ErgoBox(
        value = 1000000000L,
        ergoTree = ErgoTree.fromProposition(TrueProp),
        additionalTokens = Colls.emptyColl,
        additionalRegisters = Map.empty,
        transactionId = bytesToId(Algos.hash("header-size-codec-source")),
        index = 0.toShort,
        creationHeight = 0
      )
      val tx = new ErgoTransaction(IndexedSeq(Input(source.id, ProverResult.empty)), IndexedSeq.empty,
        IndexedSeq(new ErgoBoxCandidate(source.value, source.ergoTree, 0, source.additionalTokens, Map.empty)))
      val section = BlockTransactions(recovered.header.id, recovered.header.version, Seq(tx))
      val block = ErgoFullBlock(recovered.header, section, Extension(recovered.header.id, Seq.empty), None)
      // Preserve the existing full-block size definition, which does not count the extension.
      val expectedBlockSize = headerBytes.length + section.bytes.length
      block.id shouldBe original.id
      block.size shouldBe expectedBlockSize
      ErgoFullBlock.jsonEncoder(block).hcursor.get[Int]("size") shouldBe Right(expectedBlockSize)
      ErgoFullBlock.blockSizeEncoder(block).hcursor.get[Int]("size") shouldBe Right(expectedBlockSize)
      ErgoFullBlock.jsonEncoder(block).hcursor.downField("header").get[Int]("size") shouldBe Right(headerBytes.length)
      Header.jsonEncoder(recovered.header).hcursor.get[Int]("size") shouldBe Right(headerBytes.length)
      recovered.header.sizeOpt shouldBe Some(headerBytes.length)
      recovered.header.size shouldBe headerBytes.length
    }
  }
}
