package org.ergoplatform.nodeView.history.storage

import org.ergoplatform.nodeView.history.extra.ExtraIndex
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.iq80.leveldb.{DB, Options}
import scorex.db.{LDBFactory, LDBKVStore}

import java.nio.file.Files
import java.lang.management.ManagementFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.util.Try

class HistoryStorageCloseOverlapSpec extends ErgoCorePropertyTest {
  import org.ergoplatform.utils.ErgoNodeTestConstants.settings

  property("extra batch completes and storage closes without a paused update") {
    val f = new Fixture(false)
    try {
      f.storage.insertExtraTry(Array(Array[Byte](1) -> Array[Byte](2)), Array.empty[ExtraIndex]).get
      f.extraStore.get(Array[Byte](1)).get.toSeq shouldBe Seq[Byte](2)
    } finally f.closeStorage()
    f.closeEntered.getCount shouldBe 0L
  }

  property("close must not enter the extra store while an extra batch is in flight") {
    val f = new Fixture(true)
    val failure = new AtomicReference[Throwable]()
    def worker(body: => Unit): Thread = {
      val thread = new Thread(new Runnable {
        override def run(): Unit = try body catch { case t: Throwable => failure.compareAndSet(null, t) }
      })
      thread.setDaemon(true)
      thread
    }
    val writer = worker {
      f.storage.insertExtraTry(Array(Array[Byte](1) -> Array[Byte](2)), Array.empty[ExtraIndex]).get
    }
    val closer = worker { f.closeStorage() }
    var writerStarted = false
    var closerStarted = false
    var closeReachedStorage = false
    var enteredWhileUpdatePaused = false
    try {
      writer.start()
      writerStarted = true
      f.updateEntered.await(5, TimeUnit.SECONDS) shouldBe true
      closer.start()
      closerStarted = true
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      while (!closeReachedStorage && System.nanoTime() < deadline) {
        val info = ManagementFactory.getThreadMXBean.getThreadInfo(closer.getId, 100)
        val blockedByWriter = info != null && info.getThreadState == Thread.State.BLOCKED &&
          info.getLockOwnerId == writer.getId && info.getLockInfo != null &&
          info.getLockInfo.getIdentityHashCode == System.identityHashCode(f.storage) &&
          info.getStackTrace.exists { frame =>
            frame.getClassName == classOf[HistoryStorage].getName && frame.getMethodName == "close"
          }
        closeReachedStorage = f.closeEntered.getCount == 0 || blockedByWriter
        if (!closeReachedStorage) Thread.`yield`()
      }
      closeReachedStorage shouldBe true
      writer.isAlive shouldBe true
      enteredWhileUpdatePaused = f.closeEntered.await(300, TimeUnit.MILLISECONDS)
    } finally {
      f.resumeUpdate.countDown()
      if (writerStarted) writer.join(5000)
      if (!writerStarted || !writer.isAlive) {
        if (!closerStarted) {
          closer.start()
          closerStarted = true
        }
      }
      if (closerStarted) closer.join(5000)
      if (writerStarted && writer.isAlive)
        throw new IllegalStateException("writer did not terminate; cleanup did not start a new closer")
    }
    writer.isAlive shouldBe false
    closer.isAlive shouldBe false
    Option(failure.get()).foreach { error => throw error }
    withClue("extraStore.close entered before the in-flight batch was released: ") {
      enteredWhileUpdatePaused shouldBe false
    }
    f.closeEntered.getCount shouldBe 0L
  }

  private class Fixture(pause: Boolean) {
    private val root = Files.createTempDirectory("history-extra-close-overlap")
    val updateEntered = new CountDownLatch(1)
    val resumeUpdate = new CountDownLatch(if (pause) 1 else 0)
    val updateFinished = new CountDownLatch(if (pause) 1 else 0)
    val closeEntered = new CountDownLatch(1)
    private var indexStore: LDBKVStore = _
    private var objectsStore: LDBKVStore = _
    var extraStore: LDBKVStore = _

    private class TrackedStore(db: DB) extends LDBKVStore(db) {
      private val closed = new AtomicBoolean(false)
      override def close(): Unit = if (closed.compareAndSet(false, true)) super.close()
    }

    private def openStore(name: String)(wrap: DB => LDBKVStore): LDBKVStore = {
      val raw = LDBFactory.factory.open(root.resolve(name).toFile, new Options().createIfMissing(true))
      try wrap(raw) catch {
        case t: Throwable =>
          try raw.close() catch { case closeError: Throwable =>
            if (t ne closeError) t.addSuppressed(closeError)
          }
          throw t
      }
    }

    def closeRemaining(): Unit = {
      var failure: Throwable = null
      Seq(extraStore, indexStore, objectsStore).filter(_ != null).foreach { store =>
        try store.close() catch {
          case t: Throwable =>
            if (failure == null) failure = t
            else if (failure ne t) failure.addSuppressed(t)
        }
      }
      if (failure != null) throw failure
    }

    def closeStorage(): Unit = {
      var failure: Throwable = null
      try storage.close() catch { case t: Throwable => failure = t }
      try closeRemaining() catch {
        case t: Throwable =>
          if (failure == null) failure = t
          else if (failure ne t) failure.addSuppressed(t)
      }
      if (failure != null) throw failure
    }

    private def openExtra(db: DB): LDBKVStore = new TrackedStore(db) {
      override def update(keys: Array[Array[Byte]], values: Array[Array[Byte]],
                          remove: Array[Array[Byte]]): Try[Unit] = {
        updateEntered.countDown()
        try {
          require(resumeUpdate.await(15, TimeUnit.SECONDS), "timed out waiting to release extra batch")
          super.update(keys, values, remove)
        } finally updateFinished.countDown()
      }

      override def close(): Unit = {
        closeEntered.countDown()
        if (updateEntered.getCount == 0)
          require(updateFinished.await(15, TimeUnit.SECONDS), "timed out waiting for extra batch")
        super.close()
      }
    }
    val storage: HistoryStorage = try {
      indexStore = openStore("index")(db => new TrackedStore(db))
      objectsStore = openStore("objects")(db => new TrackedStore(db))
      extraStore = openStore("extra")(openExtra)
      new HistoryStorage(indexStore, objectsStore, extraStore, settings.cacheSettings)
    } catch {
      case t: Throwable =>
        try closeRemaining() catch { case closeError: Throwable =>
          if (t ne closeError) t.addSuppressed(closeError)
        }
        throw t
    }
  }
}
