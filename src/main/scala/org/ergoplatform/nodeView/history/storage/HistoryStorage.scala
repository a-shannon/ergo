package org.ergoplatform.nodeView.history.storage

import com.github.benmanes.caffeine.cache.Caffeine
import org.ergoplatform.modifiers.{BlockSection, NetworkObjectTypeId}
import org.ergoplatform.modifiers.history.HistoryModifierSerializer
import org.ergoplatform.modifiers.history.header.Header
import org.ergoplatform.nodeView.history.extra.{ExtraIndex, ExtraIndexSerializer, IndexedErgoBox, Segment, StorageRentBox}
import org.ergoplatform.settings.{Algos, CacheSettings, ErgoSettings}
import org.ergoplatform.utils.ScorexEncoding
import scorex.db.{ByteArrayWrapper, LDBFactory, LDBKVStore}
import scorex.util.{ModifierId, ScorexLogging, idToBytes}

import scala.util.{Failure, Success, Try}
import spire.syntax.all.cfor

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/** A rent-index page bounded by raw extra-store keys, including non-rent IDs. */
case class StorageRentScanPage(rows: Array[StorageRentBox],
                               after: Option[Vector[Byte]],
                               hasMore: Boolean,
                               rawKeysRead: Int)

/**
  * Storage for Ergo history
  *
  * @param indexStore   - Additional key-value storage for indexes, required by History for efficient work.
  *                     contains links to bestHeader, bestFullBlock, heights and scores for different blocks, etc.
  * @param objectsStore - key-value store, where key is id of ErgoPersistentModifier and value is it's bytes
  * @param extraStore   - key-value store, where key is id of Index and value is it's bytes
  * @param config       - cache configs
  */
class HistoryStorage(indexStore: LDBKVStore, objectsStore: LDBKVStore, extraStore: LDBKVStore, config: CacheSettings)
  extends ScorexLogging
    with AutoCloseable
    with ScorexEncoding {

  private lazy val headersCache =
    Caffeine.newBuilder()
      .maximumSize(config.history.headersCacheSize)
      .build[String, BlockSection]()

  private lazy val blockSectionsCache =
    Caffeine.newBuilder()
      .maximumSize(config.history.blockSectionsCacheSize)
      .build[String, BlockSection]()

  private lazy val extraCache =
    Caffeine.newBuilder()
      .maximumSize(config.history.extraCacheSize)
      .build[String, ExtraIndex]()

  private lazy val indexCache =
    Caffeine.newBuilder()
      .maximumSize(config.history.indexesCacheSize)
      .build[ByteArrayWrapper, Array[Byte]]

  private def cacheModifier(mod: BlockSection): Unit = mod.modifierTypeId match {
    case Header.modifierTypeId => headersCache.put(mod.id, mod)
    case _ => blockSectionsCache.put(mod.id, mod)
  }

  private def lookupModifier(id: ModifierId): Option[BlockSection] =
    Option(headersCache.getIfPresent(id)) orElse Option(blockSectionsCache.getIfPresent(id))

  private def removeModifier(id: ModifierId): Unit = {
    headersCache.invalidate(id)
    blockSectionsCache.invalidate(id)
    extraCache.invalidate(id)
  }

  def modifierBytesById(id: ModifierId): Option[Array[Byte]] = {
    objectsStore.get(idToBytes(id)).map(_.tail).orElse(extraStore.get(idToBytes(id))) // removing modifier type byte with .tail (only in objectsStore)
  }

  /**
    * @return bytes and type of a network object stored in the database with identifier `id`
    */
  def modifierTypeAndBytesById(id: ModifierId): Option[(NetworkObjectTypeId.Value, Array[Byte])] = {
    objectsStore.get(idToBytes(id)).map(bs => (NetworkObjectTypeId.fromByte(bs.head), bs.tail)) // first byte is type id, tail is modifier bytes
  }

  def modifierById(id: ModifierId): Option[BlockSection] =
    lookupModifier(id) orElse objectsStore.get(idToBytes(id)).flatMap { bytes =>
      HistoryModifierSerializer.parseBytesTry(bytes) match {
        case Success(pm) =>
          log.trace(s"Cache miss for existing modifier $id")
          cacheModifier(pm)
          Some(pm)
        case Failure(e) =>
          log.warn(s"Failed to parse modifier ${encoder.encode(id)} from db (bytes are: ${Algos.encode(bytes)})", e)
          None
      }
    }

  def getExtraIndex(id: ModifierId): Option[ExtraIndex] = {
    Option(extraCache.getIfPresent(id)) orElse extraStore.get(idToBytes(id)).flatMap { bytes =>
      ExtraIndexSerializer.parseBytesTry(bytes) match {
        case Success(pm) =>
          log.trace(s"Cache miss for existing index $id")
          if(!pm.isInstanceOf[Segment[_]]){
            extraCache.put(pm.id, pm) // cache non-segment objects
          }
          Some(pm)
        case Failure(_) =>
          log.warn(s"Failed to parse index ${encoder.encode(id)} from db (bytes are: ${Algos.encode(bytes)})")
          None
      }
    }
  }

  def getIndex(id: ByteArrayWrapper): Option[Array[Byte]] =
    Option(indexCache.getIfPresent(id)).orElse {
      indexStore.get(id.data).map { value =>
        indexCache.put(id, value)
        value
      }
    }

  /**
    * Remove storage-rent eligibility entries of the given boxes. This persistent operation
    * must not be used for speculative candidate rejection or before the indexer has accepted
    * the selected-chain spend. An entry whose [[IndexedErgoBox]] is absent cannot be located.
    */
  def removeStorageRentBoxes(boxIds: Seq[ModifierId]): Unit = {
    val keys = boxIds.flatMap { boxId =>
      getExtraIndex(boxId) match {
        case Some(iEb: IndexedErgoBox) => Some(StorageRentBox(iEb).id)
        case _ =>
          log.warn(s"Can not remove storage-rent eligibility entry of box $boxId, box not indexed")
          None
      }
    }
    if (keys.nonEmpty) removeExtra(keys.toArray)
  }

  /**
    * Read up to `limit` storage-rent eligibility entries for currently-unspent boxes created
    * at or before `creationHeight`, in ascending (creationHeight, globalIndex) order.
    * Returns an empty array when the extra index is disabled or does not cover the height.
    */
  def storageRentBoxesUntil(creationHeight: Int, limit: Int): Array[StorageRentBox] = {
    storageRentBoxesAfter(creationHeight, limit, None)
  }

  private def rentRowWithin(key: Array[Byte], creationHeight: Int): Boolean =
    key.length == StorageRentBox.KeyLength &&
      key(0) == StorageRentBox.KeyMarker &&
      java.nio.ByteBuffer.wrap(key, 1, 4).getInt <= creationHeight

  // A 32-byte ID can sort inside the rent-key prefix. Only crossing the ordered
  // height prefix or leaving the marker namespace ends a scan.
  private def inRentHeightPrefix(key: Array[Byte], creationHeight: Int): Boolean =
    key.nonEmpty && key(0) == StorageRentBox.KeyMarker &&
      (key.length < 5 || java.lang.Integer.compareUnsigned(
        java.nio.ByteBuffer.wrap(key, 1, 4).getInt, creationHeight) <= 0)

  /**
    * Resume a rent-row-limited index scan strictly after the supplied key. The cursor remains
    * valid if that row was spent and removed between scans: LevelDB seeks to its successor.
    */
  def storageRentBoxesAfter(creationHeight: Int,
                            limit: Int,
                            after: Option[(Int, Long)]): Array[StorageRentBox] = {
    val start = after.map { case (height, index) => StorageRentBox.key(height, index) }
      .getOrElse(StorageRentBox.key(0, 0L))
    extraStore.scanFrom(
      start,
      limit,
      keyFilter = key => rentRowWithin(key, creationHeight) &&
        !after.exists(_ => java.util.Arrays.equals(key, start)),
      continueScan = key => inRentHeightPrefix(key, creationHeight)
    ).map { case (_, bytes) =>
      ExtraIndexSerializer.parseBytes(bytes).asInstanceOf[StorageRentBox]
    }
  }

  /**
    * Inspect at most `rawLimit + 2` raw extra-store keys, including any
    * range-boundary key. Resume strictly after the last page key; a deleted
    * cursor seeks to its successor. The two extra keys cover an inclusive
    * cursor and one lookahead, even after deletion.
    */
  def storageRentBoxesPage(creationHeight: Int,
                           rawLimit: Int,
                           after: Option[Vector[Byte]]): StorageRentScanPage = {
    require(rawLimit > 0 && rawLimit <= Int.MaxValue - 2)
    val start = after.map(_.toArray).getOrElse(StorageRentBox.key(0, 0L))
    var rawKeysRead = 0
    val scanned = extraStore.scanFrom(start, rawLimit + 2,
      keyFilter = _ => true,
      continueScan = key => {
        rawKeysRead += 1
        inRentHeightPrefix(key, creationHeight)
      })
    val fresh = if (scanned.headOption.exists { case (key, _) =>
      after.exists(cursor => java.util.Arrays.equals(key, cursor.toArray))
    }) scanned.tail else scanned
    val visited = fresh.take(rawLimit)
    val hasMore = fresh.length > rawLimit
    val rows = visited.collect { case (key, bytes) if rentRowWithin(key, creationHeight) =>
      ExtraIndexSerializer.parseBytes(bytes).asInstanceOf[StorageRentBox]
    }
    StorageRentScanPage(rows,
      if (hasMore) visited.lastOption.map(_._1.toVector) else None,
      hasMore, rawKeysRead)
  }

  /**
    * @return object with `id` if it is in the objects database
    */
  def get(id: ModifierId): Option[Array[Byte]] = {
    val idBytes = idToBytes(id)
    objectsStore.get(idBytes).orElse(extraStore.get(idBytes))
  }
  def get(id: Array[Byte]): Option[Array[Byte]] = objectsStore.get(id).orElse(extraStore.get(id))

  /**
    * @return if object with `id` is in the objects database
    */
  def contains(id: Array[Byte]): Boolean = get(id).isDefined
  def contains(id: ModifierId): Boolean = get(id).isDefined

  def insert(indexesToInsert: Array[(ByteArrayWrapper, Array[Byte])],
             objectsToInsert: Array[BlockSection]): Try[Unit] = {
    objectsStore.insert(
      objectsToInsert.map(mod => mod.serializedId),
      objectsToInsert.map(mod => HistoryModifierSerializer.toBytes(mod))
    ).flatMap { _ =>
      cfor(0)(_ < objectsToInsert.length, _ + 1) { i => cacheModifier(objectsToInsert(i))}
      if (indexesToInsert.nonEmpty) {
        indexStore.insert(
          indexesToInsert.map(_._1.data),
          indexesToInsert.map(_._2)
        ).map { _ =>
          cfor(0)(_ < indexesToInsert.length, _ + 1) { i =>
            indexCache.put(indexesToInsert(i)._1, indexesToInsert(i)._2)
          }
        }
      } else Success(())
    }
  }

  def insertExtra(indexesToInsert: Array[(Array[Byte], Array[Byte])],
                  objectsToInsert: Array[ExtraIndex]): Unit = {
    extraStore.insert(
      objectsToInsert.map(mod => mod.serializedId),
      objectsToInsert.map(mod => ExtraIndexSerializer.toBytes(mod))
    )
    cfor(0)(_ < indexesToInsert.length, _ + 1) { i => extraStore.insert(indexesToInsert(i)._1, indexesToInsert(i)._2)}
  }

  def removeExtra(indexesToRemove: Array[ModifierId]) : Unit = {
    extraStore.remove(indexesToRemove.map(idToBytes))
    cfor(0)(_ < indexesToRemove.length, _ + 1) { i => removeModifier(indexesToRemove(i)) }
  }

  /**
    * Insert single object to database. This version allows for efficient insert
    * when identifier and bytes of object (i.e. modifier, a block section) are known.
    *
    * @param objectIdToInsert - object id to insert
    * @param objectToInsert - object bytes to insert
    * @return - Success if insertion was successful, Failure otherwise
    */
  def insert(objectIdToInsert: Array[Byte],
             objectToInsert: Array[Byte]): Try[Unit] = {
    objectsStore.insert(objectIdToInsert, objectToInsert)
  }

  /**
    * Remove elements from stored indices and modifiers
    *
    * @param indicesToRemove - indices keys to remove
    * @param idsToRemove - identifiers of modifiers to remove
    * @return
    */
  def remove(indicesToRemove: Array[ByteArrayWrapper],
             idsToRemove: Array[ModifierId]): Try[Unit] = {

      objectsStore.remove(idsToRemove.map(idToBytes)).map { _ =>
        cfor(0)(_ < idsToRemove.length, _ + 1) { i => removeModifier(idsToRemove(i))}
        indexStore.remove(indicesToRemove.map(_.data)).map { _ =>
          cfor(0)(_ < indicesToRemove.length, _ + 1) { i => indexCache.invalidate(indicesToRemove(i))}
          ()
        }
      }
  }

  override def close(): Unit = {
    log.warn("Closing history storage...")
    extraStore.close()
    indexStore.close()
    objectsStore.close()
  }

  /**
    * Delete the extra index database and reopen it.
    *
    * @param ergoSettings - settings to use
    * @return new HistoryStorage instance with empty extra database, or this instance in case of failure
    */
  def deleteExtraDB(ergoSettings: ErgoSettings): HistoryStorage = {
    log.warn(s"Removing extra index database due to old schema.")
    close()
    // org.ergoplatform.wallet.utils.FileUtils
    val root = new File(s"${ergoSettings.directory}/history/extra")
    if (root.exists()) {
      Files.walk(root.toPath).iterator().asScala.toSeq.reverse.foreach(path => Try(Files.delete(path)))
    }else {
      log.error(s"Could not delete ${root.toString}")
      return this
    }
    log.info(s"Deleted ${root.toString}")
    HistoryStorage.apply(ergoSettings)
  }

}

object HistoryStorage {
  def apply(ergoSettings: ErgoSettings): HistoryStorage = {
    val indexStore = LDBFactory.createKvDb(s"${ergoSettings.directory}/history/index")
    val objectsStore = LDBFactory.createKvDb(s"${ergoSettings.directory}/history/objects")
    val extraStore = LDBFactory.createKvDb(s"${ergoSettings.directory}/history/extra")
    new HistoryStorage(indexStore, objectsStore, extraStore, ergoSettings.cacheSettings)
  }
}
