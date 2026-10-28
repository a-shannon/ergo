package org.ergoplatform.nodeView.history.storage

import org.ergoplatform.nodeView.history.extra.ExtraIndex
import org.ergoplatform.utils.ErgoCorePropertyTest
import scorex.db.{ByteArrayWrapper, LDBKVStore}
import scorex.util.bytesToId

import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.util.Try

class HistoryStorageLifecycleSpec extends ErgoCorePropertyTest {
  import org.ergoplatform.utils.ErgoNodeTestConstants.settings

  private class Store extends LDBKVStore(null) {
    val closes = new AtomicInteger(0)
    val updates = new AtomicInteger(0)
    var closeError: Throwable = null
    var updateError: Throwable = null
    var updateEntered = new CountDownLatch(0)
    var releaseUpdate = new CountDownLatch(0)
    var closeEntered = new CountDownLatch(0)
    var releaseClose = new CountDownLatch(0)

    override def get(key: Array[Byte]): Option[Array[Byte]] = Some(Array[Byte](7))
    override def update(keys: Array[Array[Byte]], values: Array[Array[Byte]],
                        remove: Array[Array[Byte]]): Try[Unit] = {
      if (updateError != null) throw updateError
      Try {
        updates.incrementAndGet()
        updateEntered.countDown()
        require(releaseUpdate.await(5, TimeUnit.SECONDS))
      }
    }
    override def close(): Unit = {
      closes.incrementAndGet()
      closeEntered.countDown()
      require(releaseClose.await(5, TimeUnit.SECONDS))
      if (closeError != null) throw closeError
    }
  }

  property("remove propagates an exception thrown before the store returns a Try") {
    val extra = new Store
    val index = new Store
    val objects = new Store
    val storage = new HistoryStorage(index, objects, extra, settings.cacheSettings)
    val id = bytesToId(Array.fill[Byte](32)(1))
    val error = new IOException("update failed before returning")
    try {
      storage.remove(Array.empty[ByteArrayWrapper], Array(id)).get
      objects.updateError = error
      intercept[IOException] {
        storage.remove(Array.empty[ByteArrayWrapper], Array(id))
      } shouldBe error
    } finally {
      storage.close()
      Seq(extra, index, objects).map(_.closes.get()) shouldBe Seq(1, 1, 1)
    }
  }

  private def thread(body: => Unit, failure: AtomicReference[Throwable]): Thread = {
    val t = new Thread(new Runnable {
      override def run(): Unit = try body catch { case e: Throwable => failure.compareAndSet(null, e) }
    })
    t.setDaemon(true)
    t
  }

  private def blockedOnStorage(waiter: Thread, owner: Thread, storage: HistoryStorage): Boolean = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    var blocked = false
    while (!blocked && waiter.isAlive && System.nanoTime() < deadline) {
      val info = ManagementFactory.getThreadMXBean.getThreadInfo(waiter.getId)
      blocked = info != null && info.getThreadState == Thread.State.BLOCKED &&
        info.getLockOwnerId == owner.getId && info.getLockInfo != null &&
        info.getLockInfo.getIdentityHashCode == System.identityHashCode(storage)
      if (!blocked) Thread.`yield`()
    }
    blocked
  }

  property("an in-flight operation completes before close enters a store") {
    val extra = new Store
    val index = new Store
    val objects = new Store
    val storage = new HistoryStorage(index, objects, extra, settings.cacheSettings)
    extra.updateEntered = new CountDownLatch(1)
    extra.releaseUpdate = new CountDownLatch(1)
    extra.closeEntered = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val writer = thread({
      storage.insertExtraTry(Array(Array[Byte](1) -> Array[Byte](2)), Array.empty[ExtraIndex]).get
    }, failure)
    val closer = thread(storage.close(), failure)
    try {
      writer.start()
      extra.updateEntered.await(5, TimeUnit.SECONDS) shouldBe true
      closer.start()
      blockedOnStorage(closer, writer, storage) shouldBe true
      extra.closeEntered.getCount shouldBe 1L
    } finally {
      extra.releaseUpdate.countDown()
      writer.join(5000)
      if (closer.getState == Thread.State.NEW) closer.start()
      closer.join(5000)
    }
    writer.isAlive shouldBe false
    closer.isAlive shouldBe false
    Option(failure.get()).foreach(error => throw error)
    extra.closes.get() shouldBe 1
  }

  property("close-first blocks an operation and post-close reads reject cached data") {
    val extra = new Store
    val index = new Store
    val objects = new Store
    val storage = new HistoryStorage(index, objects, extra, settings.cacheSettings)
    val key = ByteArrayWrapper(Array[Byte](1))
    storage.getIndex(key).isDefined shouldBe true
    extra.closeEntered = new CountDownLatch(1)
    extra.releaseClose = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val result = new AtomicReference[Try[Unit]]()
    val closer = thread(storage.close(), failure)
    val writer = thread({
      result.set(storage.insertExtraTry(Array(Array[Byte](1) -> Array[Byte](2)), Array.empty[ExtraIndex]))
    }, failure)
    try {
      closer.start()
      extra.closeEntered.await(5, TimeUnit.SECONDS) shouldBe true
      writer.start()
      blockedOnStorage(writer, closer, storage) shouldBe true
    } finally {
      extra.releaseClose.countDown()
      closer.join(5000)
      if (writer.getState == Thread.State.NEW) writer.start()
      writer.join(5000)
    }
    closer.isAlive shouldBe false
    writer.isAlive shouldBe false
    Option(failure.get()).foreach(error => throw error)
    result.get().isFailure shouldBe true
    storage.insertExtra(Array(Array[Byte](1) -> Array[Byte](2)), Array.empty[ExtraIndex])
    storage.removeExtra(Array.empty)
    extra.updates.get() shouldBe 0
    intercept[IllegalStateException](storage.getIndex(key))
    intercept[IllegalStateException](storage.get(Array[Byte](1)))
    intercept[IllegalStateException](storage.storageRentBoxesAtOrBefore(0, 1))
    storage.close()
    Seq(extra, index, objects).map(_.closes.get()) shouldBe Seq(1, 1, 1)
  }

  property("close attempts every store once and failed close prevents deletion") {
    val extra = new Store
    val index = new Store
    val objects = new Store
    val first = new IllegalStateException("extra close")
    val second = new IllegalStateException("index close")
    extra.closeError = first
    index.closeError = second
    val storage = new HistoryStorage(index, objects, extra, settings.cacheSettings)
    val root = Files.createTempDirectory("history-close-failure")
    val extraDir = Files.createDirectories(root.resolve("history").resolve("extra"))
    val marker = Files.write(extraDir.resolve("marker"), Array[Byte](1))
    intercept[IllegalStateException](storage.close()) shouldBe first
    first.getSuppressed.toSeq shouldBe Seq(second)
    intercept[IllegalStateException](storage.close()) shouldBe first
    storage.deleteExtraDBTry(settings.copy(directory = root.toString)).failed.get.getCause shouldBe first
    Files.exists(marker) shouldBe true
    storage.removeExtraTry(Array.empty).isFailure shouldBe true
    intercept[IllegalStateException](storage.get(Array[Byte](1))).getCause shouldBe first
    Seq(extra, index, objects).map(_.closes.get()) shouldBe Seq(1, 1, 1)
  }

  property("an old instance cannot delete its live replacement's extra database") {
    val root = Files.createTempDirectory("history-extra-replacement")
    val localSettings = settings.copy(directory = root.toString)
    val original = HistoryStorage(localSettings)
    val replacement = original.deleteExtraDBTry(localSettings).get
    try {
      val key = Array[Byte](1, 2, 3)
      val value = Array[Byte](4, 5, 6)
      replacement.insertExtraTry(Array(key -> value), Array.empty[ExtraIndex]).get
      val rejected = original.deleteExtraDBTry(localSettings).failed.get
      rejected.getClass shouldBe classOf[IllegalStateException]
      rejected.getMessage shouldBe "History storage is closed"
      rejected.getCause shouldBe null
      replacement.get(key).map(_.toSeq) shouldBe Some(value.toSeq)
      Files.exists(root.resolve("history").resolve("extra")) shouldBe true
    } finally replacement.close()
  }
}
