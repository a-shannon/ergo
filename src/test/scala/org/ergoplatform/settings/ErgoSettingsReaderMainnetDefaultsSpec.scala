package org.ergoplatform.settings

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.control.NonFatal

class ErgoSettingsReaderMainnetDefaultsSpec extends AnyFlatSpec with Matchers {
  "The MainNet settings reader" should "resolve production defaults without a user file" in {
    val application = new File("src/main/resources/application.conf").getCanonicalFile
    application.isFile shouldBe true

    val javaExecutable = Paths.get(
      System.getProperty("java.home"),
      "bin",
      if (scala.util.Properties.isWin) "java.exe" else "java"
    ).toString
    // The reader writes mainnet.conf to java.io.tmpdir. Keep that file and its
    // shutdown hook isolated from other settings tests.
    val temporaryDirectory = Files.createTempDirectory("ergo-mainnet-reader-").toFile
    temporaryDirectory.deleteOnExit()
    val markers = new java.lang.StringBuffer
    val logger = ProcessLogger(
      line => if (line.startsWith("READER_")) markers.append(line).append('\n'),
      _ => ()
    )
    val exitCode = Process(Seq(
      javaExecutable,
      s"-Djava.io.tmpdir=${temporaryDirectory.getAbsolutePath}",
      // The normal test classpath has its own application.conf. Select the
      // bundled production application.conf for this release check.
      s"-Dconfig.file=${application.getAbsolutePath}",
      "-cp",
      System.getProperty("java.class.path"),
      "org.ergoplatform.settings.ErgoSettingsReaderMainnetDefaultsProbe"
    )).!(logger)

    withClue(s"reader markers: $markers") {
      markers.toString should include("READER_CONTROL_OK")
      exitCode shouldBe 0
      markers.toString should include("READER_SELECTED_OK")
    }
  }
}

object ErgoSettingsReaderMainnetDefaultsProbe {
  def main(args: Array[String]): Unit = {
    val applicationVersion = "6.0.9"
    val operatorConfig = Paths.get(System.getProperty("java.io.tmpdir"), "operator.conf")
    Files.write(operatorConfig, Array.empty[Byte])
    operatorConfig.toFile.deleteOnExit()

    def check(settings: ErgoSettings): Unit = {
      require(settings.networkType == NetworkType.MainNet)
      require(settings.scorexSettings.network.appVersion == applicationVersion)
    }

    // An empty, explicit user file takes the reader's application fallback path.
    check(ErgoSettingsReader.read(Args(Some(operatorConfig.toString), Some(NetworkType.MainNet))))
    println("READER_CONTROL_OK")

    // A selected network without a user file must resolve the same version.
    try {
      check(ErgoSettingsReader.read(Args(None, Some(NetworkType.MainNet))))
      println("READER_SELECTED_OK")
    } catch {
      case NonFatal(error) =>
        println(s"READER_SELECTED_FAILURE ${error.getClass.getSimpleName}")
        throw error
    }
  }
}
