package org.ergoplatform.nodeView.history.storage

import org.ergoplatform.modifiers.BlockSection
import org.ergoplatform.modifiers.history.ADProofs
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.nodeView.history.ErgoHistoryUtils._
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, IndexedErgoBox, StorageRentBox}
import org.ergoplatform.settings.{Algos, Constants}
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.utils.TestFileUtils
import org.scalacheck.Gen
import scorex.db.ByteArrayWrapper
import scorex.util.{ModifierId, bytesToId, idToBytes}
import sigmastate.helpers.TestingHelpers.testBox

class HistoryStorageSpec extends ErgoCorePropertyTest with TestFileUtils {
  import org.ergoplatform.utils.ErgoNodeTestConstants._
  import org.ergoplatform.utils.generators.ErgoCoreGenerators._

  val db = HistoryStorage(settings)

  property("Write Read Remove") {
    val headers: Array[Header] = Gen.listOfN(20, defaultHeaderGen).sample.get.toArray
    val modifiers: Array[ADProofs] = Gen.listOfN(20, randomADProofsGen).sample.get.toArray
    def validityKey(id: ModifierId) = ByteArrayWrapper(Algos.hash("validity".getBytes(CharsetName) ++ idToBytes(id)))
    val indexes = headers.flatMap(h => Array(validityKey(h.id) -> Array(1.toByte)))
    db.insert(indexes, (headers ++ modifiers).asInstanceOf[Array[BlockSection]]) shouldBe 'success

    headers.forall(h => db.contains(h.id)) shouldBe true
    modifiers.forall(m => db.contains(m.id)) shouldBe true

    headers.forall(h => db.get(h.id).exists(_.nonEmpty)) shouldBe true
    modifiers.forall(m => db.get(m.id).exists(_.nonEmpty)) shouldBe true
    indexes.forall(i => db.getIndex(i._1).exists(_.nonEmpty)) shouldBe true

    db.remove(indexes.map(_._1), headers.map(_.id) ++ modifiers.map(_.id))

    headers.forall(h => !db.contains(h.id)) shouldBe true
    modifiers.forall(m => !db.contains(m.id)) shouldBe true

    headers.forall(h => !db.get(h.id).exists(_.nonEmpty)) shouldBe true
    modifiers.forall(m => !db.get(m.id).exists(_.nonEmpty)) shouldBe true
    indexes.forall(i => !db.getIndex(i._1).exists(_.nonEmpty)) shouldBe true
  }

  /** Insert a distinct box and its storage-rent eligibility entry, return the box row. */
  private def insertRentBox(globalIndex: Long): IndexedErgoBox = {
    val creationHeight = 1000 + globalIndex.toInt
    val box = testBox(1000000000L, Constants.TrueTree, creationHeight)
    val iEb = new IndexedErgoBox(creationHeight, None, None, None, box, globalIndex)
    db.insertExtra(Array.empty, Array[ExtraIndex](iEb, StorageRentBox(iEb)))
    iEb
  }

  private def rentBoxIdsInIndex(limit: Int): Seq[ModifierId] =
    db.storageRentBoxesUntil(Int.MaxValue, limit).map(_.boxId).toSeq

  property("storage rent entries are removable by box id") {
    val iEbs = (0L until 3).map(insertRentBox)
    val boxIds = iEbs.map(_.id)
    rentBoxIdsInIndex(10).toSet shouldBe boxIds.toSet

    // removing a subset removes exactly those entries, in key order for the rest
    db.removeStorageRentBoxes(boxIds.take(2))
    rentBoxIdsInIndex(10) shouldBe Seq(boxIds(2))

    // repeated removal is a no-op; unknown box ids are ignored
    db.removeStorageRentBoxes(boxIds.take(2) :+ bytesToId(Array.fill(32)(42.toByte)))
    rentBoxIdsInIndex(10) shouldBe Seq(boxIds(2))

    // an entry whose IndexedErgoBox is gone can not be located, so it is left in place
    db.removeExtra(Array(boxIds(2)))
    db.removeStorageRentBoxes(Seq(boxIds(2)))
    rentBoxIdsInIndex(10) shouldBe Seq(boxIds(2))
  }

  property("rent pages resume after a key even when its row was deleted") {
    val storage = HistoryStorage(settings.copy(directory = createTempDir.getAbsolutePath))
    val rows = (0L until 3).map { index =>
      val height = 100 + index.toInt
      val box = testBox(1000000000L, Constants.TrueTree, height)
      val indexed = new IndexedErgoBox(height, None, None, None, box, index)
      storage.insertExtra(Array.empty, Array[ExtraIndex](indexed, StorageRentBox(indexed)))
      indexed
    }
    try {
      val first = rows.head
      val second = rows(1)
      val afterFirst = Some(first.box.creationHeight -> first.globalIndex)
      // The extra store also contains 32-byte IDs. A same-marker ID can sort between
      // two rent keys and must not terminate the rent namespace scan.
      val interleavedId = StorageRentBox.key(first.box.creationHeight, first.globalIndex) ++
        Array.fill(19)(1.toByte)
      storage.insertExtra(Array(interleavedId -> Array(1.toByte)), Array.empty[ExtraIndex])
      storage.storageRentBoxesAfter(101, 3, None).map(_.boxId).toSeq shouldBe
        Seq(first.id, second.id)
      storage.storageRentBoxesAfter(101, 1, None).map(_.boxId).toSeq shouldBe Seq(first.id)
      storage.storageRentBoxesAfter(101, 2, afterFirst).map(_.boxId).toSeq shouldBe Seq(second.id)
      storage.removeExtra(Array(StorageRentBox(first).id))
      storage.storageRentBoxesAfter(101, 2, afterFirst).map(_.boxId).toSeq shouldBe Seq(second.id)
      storage.storageRentBoxesAfter(101, 2,
        Some(second.box.creationHeight -> second.globalIndex)) shouldBe empty
    } finally {
      storage.close()
    }
  }

  property("raw rent pages bound interleaved foreign keys and resume after a deleted cursor") {
    val storage = HistoryStorage(settings.copy(directory = createTempDir.getAbsolutePath))
    val rows = (0L until 2).map { index =>
      val height = 100 + index.toInt
      val box = testBox(1000000000L, Constants.TrueTree, height)
      val rent = new StorageRentBox(height, index * 2,
        bytesToId(box.id), box.value, box.bytes.length)
      storage.insertExtra(Array.empty, Array[ExtraIndex](rent))
      rent
    }
    try {
      val foreignPrefix = StorageRentBox.key(100, 0L)
      val foreignKeys = (0 until 80).map { n =>
        foreignPrefix ++ Array(n.toByte) ++ Array.fill(18)(1.toByte)
      }
      storage.insertExtra(foreignKeys.map(_ -> Array(1.toByte)).toArray,
        Array.empty[ExtraIndex])

      val first = storage.storageRentBoxesPage(101, 7, None)
      first.rawKeysRead should be <= 9
      first.hasMore shouldBe true
      first.rows.map(_.boxId).toSeq shouldBe Seq(rows.head.boxId)
      val deletedCursor = first.after.get
      deletedCursor.length shouldBe 32
      storage.removeExtra(Array(bytesToId(deletedCursor.toArray)))

      var page = storage.storageRentBoxesPage(101, 7, Some(deletedCursor.toArray.toVector))
      var seen = first.rows.map(_.boxId).toVector
      var pageCount = 1
      var finished = false
      while (pageCount < 20 && !finished) {
        page.rawKeysRead should be <= 9
        seen ++= page.rows.map(_.boxId)
        pageCount += 1
        finished = !page.hasMore
        if (!finished) page = storage.storageRentBoxesPage(101, 7, page.after)
      }
      finished shouldBe true
      pageCount should be > 10
      seen shouldBe Vector(rows.head.boxId, rows(1).boxId)
    } finally {
      storage.close()
    }
  }

}
