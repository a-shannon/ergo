package org.ergoplatform.wallet.secrets

import org.ergoplatform.sdk.SecretString
import org.ergoplatform.sdk.wallet.settings.EncryptionSettings
import org.ergoplatform.wallet.settings.SecretStorageSettings
import org.ergoplatform.wallet.utils.FileUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

import java.io.{File, IOException, Writer}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path}

class JsonSecretStoragePersistenceSpec extends AnyPropSpec with Matchers with FileUtils {
  private val encryption = EncryptionSettings("HmacSHA256", 1, 256)
  private val contents = """{"test":"checked persistence"}"""

  private def entries(dir: File): Set[String] = dir.listFiles().map(_.getName).toSet

  property("wallet discovery ignores an unpublished staging file") {
    val dir = createTempDir
    Files.createFile(dir.toPath.resolve(".ergo-secret-staging-example.tmp"))

    JsonSecretStorage.readFile(SecretStorageSettings(dir.getAbsolutePath, encryption)) shouldBe 'failure
  }

  property("initialization erases its seed when the secret directory cannot be created") {
    val parent = createTempDir
    val occupied = Files.createFile(parent.toPath.resolve("occupied"))
    val seed = Array.fill[Byte](32)(1)
    val settings = SecretStorageSettings(occupied.resolve("wallet").toString, encryption)

    intercept[java.io.IOException] {
      JsonSecretStorage.init(seed, SecretString.create("test password"), usePre1627KeyDerivation = false)(settings)
    }

    seed shouldBe Array.fill[Byte](32)(0)
  }

  property("initialization publishes one complete wallet and erases its seed") {
    val dir = createTempDir
    val seed = Array.fill[Byte](32)(1)
    val settings = SecretStorageSettings(dir.getAbsolutePath, encryption)
    val storage = JsonSecretStorage.init(seed, SecretString.create("test password"), usePre1627KeyDerivation = false)(settings)

    entries(dir) shouldBe Set(storage.secretFile.getName)
    storage.secretFile.getName should endWith(".json")
    seed shouldBe Array.fill[Byte](32)(0)
    val reopened = JsonSecretStorage.readFile(settings).get
    reopened.unlock(SecretString.create("test password")) shouldBe 'success
    reopened.secret.get.usePre1627KeyDerivation shouldBe false
    reopened.lock()
  }

  property("wallet discovery preserves a sole legacy filename alongside staging files") {
    val dir = createTempDir
    val legacy = Files.createFile(dir.toPath.resolve("legacy-wallet"))
    val settings = SecretStorageSettings(dir.getAbsolutePath, encryption)

    JsonSecretStorage.readFile(settings).get.secretFile.toPath shouldBe legacy
    Files.createFile(dir.toPath.resolve(".ergo-secret-staging-example.tmp"))
    JsonSecretStorage.readFile(settings).get.secretFile.toPath shouldBe legacy
  }

  property("wallet publication occurs only after the checked writer closes") {
    val dir = createTempDir
    val file = new File(dir, "wallet.json")
    var closed = false
    val openWriter: Path => Writer = path => {
      file.exists() shouldBe false
      val delegate = Files.newBufferedWriter(path, UTF_8)
      new Writer {
        override def write(chars: Array[Char], offset: Int, length: Int): Unit = {
          file.exists() shouldBe false
          delegate.write(chars, offset, length)
        }
        override def flush(): Unit = delegate.flush()
        override def close(): Unit = {
          file.exists() shouldBe false
          delegate.close()
          closed = true
        }
      }
    }

    JsonSecretStorage.persist(file, contents, openWriter)

    closed shouldBe true
    new String(Files.readAllBytes(file.toPath), UTF_8) shouldBe contents
    entries(dir) shouldBe Set("wallet.json")
  }

  property("a writer open error propagates and removes only the owned staging file") {
    val dir = createTempDir
    val existing = Files.write(dir.toPath.resolve("existing.json"), contents.getBytes(UTF_8))
    val file = new File(dir, "wallet.json")
    val error = new IOException("controlled open failure")

    intercept[IOException] {
      JsonSecretStorage.persist(file, contents, _ => throw error)
    } shouldBe error

    entries(dir) shouldBe Set("existing.json")
    new String(Files.readAllBytes(existing), UTF_8) shouldBe contents
  }

  for ((failWrite, failClose) <- Seq((true, false), (false, true), (true, true))) {
    property(s"checked persistence propagates writer failures: write=$failWrite, close=$failClose") {
      val dir = createTempDir
      val file = new File(dir, "wallet.json")
      val writeError = new IOException("controlled write failure")
      val closeError = new IOException("controlled close failure")
      var closeCount = 0
      val openWriter: Path => Writer = path => {
        val delegate = Files.newBufferedWriter(path, UTF_8)
        new Writer {
          override def write(chars: Array[Char], offset: Int, length: Int): Unit = {
            if (failWrite) throw writeError
            delegate.write(chars, offset, length)
          }
          override def flush(): Unit = delegate.flush()
          override def close(): Unit = {
            closeCount += 1
            delegate.close()
            if (failClose) throw closeError
          }
        }
      }

      val thrown = intercept[IOException] {
        JsonSecretStorage.persist(file, contents, openWriter)
      }

      thrown shouldBe (if (failWrite) writeError else closeError)
      thrown.getSuppressed.toSeq shouldBe (if (failWrite && failClose) Seq(closeError) else Seq.empty)
      closeCount shouldBe 1
      file.exists() shouldBe false
      entries(dir) shouldBe Set.empty
    }
  }

  property("persistence does not overwrite an existing destination") {
    val dir = createTempDir
    val file = Files.write(dir.toPath.resolve("wallet.json"), contents.getBytes(UTF_8)).toFile

    intercept[FileAlreadyExistsException] {
      JsonSecretStorage.persist(file, "replacement")
    }

    new String(Files.readAllBytes(file.toPath), UTF_8) shouldBe contents
    entries(dir) shouldBe Set("wallet.json")
  }

  property("a publication error propagates and removes the owned staging file") {
    val dir = createTempDir
    val file = new File(dir, "wallet.json")
    val openWriter: Path => Writer = path => {
      val delegate = Files.newBufferedWriter(path, UTF_8)
      new Writer {
        override def write(chars: Array[Char], offset: Int, length: Int): Unit = delegate.write(chars, offset, length)
        override def flush(): Unit = delegate.flush()
        override def close(): Unit = {
          delegate.close()
          // A non-empty destination directory deterministically rejects file publication.
          Files.createDirectory(file.toPath)
          Files.write(file.toPath.resolve("existing"), contents.getBytes(UTF_8))
        }
      }
    }

    intercept[IOException] {
      JsonSecretStorage.persist(file, contents, openWriter)
    }

    entries(dir) shouldBe Set("wallet.json")
    new String(Files.readAllBytes(file.toPath.resolve("existing")), UTF_8) shouldBe contents
  }
}
