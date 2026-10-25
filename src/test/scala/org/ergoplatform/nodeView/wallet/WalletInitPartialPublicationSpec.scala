package org.ergoplatform.nodeView.wallet

import akka.actor.{ActorRef, Props}
import akka.testkit.TestProbe
import org.ergoplatform.nodeView.wallet.ErgoWalletActorMessages._
import org.ergoplatform.sdk.SecretString
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.{ErgoCorePropertyTest, WalletTestOps}
import org.ergoplatform.wallet.boxes.ReplaceCompactCollectBoxSelector

import java.io.File
import java.nio.file.FileAlreadyExistsException
import scala.concurrent.duration._
import scala.util.{Failure, Try}

class WalletInitPartialPublicationSpec extends ErgoCorePropertyTest with WalletTestOps {
  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  property("a failed initialization leaves the actor uninitialized until a fresh read") {
    withFixture { implicit w =>
      val root = new File(w.nodeViewDir, "partial-init")
      val walletSettings = w.settings.walletSettings
      val actorSettings = w.settings.copy(
        directory = new File(root, "node").getAbsolutePath,
        nodeSettings = w.settings.nodeSettings.copy(blocksToKeep = -1),
        walletSettings = walletSettings.copy(
          testMnemonic = None,
          secretStorage = walletSettings.secretStorage.copy(
            secretDir = new File(root, "keystore").getAbsolutePath
          )
        )
      )
      val rejected = new IllegalStateException("synthetic registry rebuild failure")
      val failingService = new ErgoWalletServiceImpl(actorSettings) {
        override def recreateRegistry(state: ErgoWalletState,
                                      settings: ErgoSettings): Try[ErgoWalletState] =
          Failure(rejected)
      }
      val selector = new ReplaceCompactCollectBoxSelector(
        actorSettings.walletSettings.maxInputs,
        actorSettings.walletSettings.optimalInputs,
        None
      )
      val probe = TestProbe()(w.actorSystem)
      val pass = "synthetic actor password"

      def newActor(service: ErgoWalletServiceImpl): ActorRef =
        w.actorSystem.actorOf(
          Props(classOf[ErgoWalletActor], actorSettings, parameters, service,
            selector, getHistory)
        )

      def closeActor(actor: ActorRef): Unit = {
        probe.watch(actor)
        probe.send(actor, CloseWallet)
        probe.expectTerminated(actor, 10.seconds)
      }

      val first = newActor(failingService)
      try {
        probe.send(first, InitWallet(SecretString.create(pass), None))
        probe.expectMsgPF(20.seconds) {
          case Failure(error: IllegalStateException) => error shouldBe rejected
        }

        val secretDir = new File(actorSettings.walletSettings.secretStorage.secretDir)
        secretDir.isDirectory shouldBe true
        secretDir.listFiles().count(_.getName.endsWith(".json")) shouldBe 1

        probe.send(first, GetWalletStatus)
        val failedStatus = probe.expectMsgType[WalletStatus]
        failedStatus.initialized shouldBe false
        failedStatus.unlocked shouldBe false

        probe.send(first, InitWallet(SecretString.create(pass), None))
        probe.expectMsgPF(20.seconds) {
          case Failure(_: FileAlreadyExistsException) => succeed
        }

        val restoreInput = SecretString.create("synthetic restore input")
        try {
          probe.send(first, RestoreWallet(restoreInput, None, SecretString.create(pass),
            usePre1627KeyDerivation = false))
          probe.expectMsgPF(20.seconds) {
            case Failure(_: FileAlreadyExistsException) => succeed
          }
        } finally restoreInput.erase()

        probe.send(first, UnlockWallet(SecretString.create(pass)))
        probe.expectMsgPF(20.seconds) {
          case Failure(error) => error.getMessage shouldBe "Wallet not initialized"
        }
      } finally closeActor(first)

      val fresh = newActor(new ErgoWalletServiceImpl(actorSettings))
      try {
        probe.send(fresh, GetWalletStatus)
        val loadedStatus = probe.expectMsgType[WalletStatus]
        loadedStatus.initialized shouldBe true
        loadedStatus.unlocked shouldBe false
      } finally closeActor(fresh)
    }
  }
}
