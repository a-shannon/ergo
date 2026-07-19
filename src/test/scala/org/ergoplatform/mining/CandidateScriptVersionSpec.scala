package org.ergoplatform.mining

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{StatusReply, ask}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.ergoplatform.mining.CandidateGenerator.{Candidate, GenerateCandidate}
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.FullBlockApplied
import org.ergoplatform.nodeView.ErgoReadersHolder.{GetReaders, Readers}
import org.ergoplatform.nodeView.state.{ErgoStateContext, StateType, UtxoStateReader}
import org.ergoplatform.nodeView.{ErgoNodeViewRef, ErgoReadersHolderRef}
import org.ergoplatform.settings.NetworkType.DevNet60
import org.ergoplatform.settings.{ErgoSettings, ErgoSettingsReader, Parameters}
import org.ergoplatform.utils.ErgoTestHelpers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sigma.VersionContext

import scala.concurrent.duration._
import scala.util.Failure

class CandidateScriptVersionSpec extends AnyFlatSpec with Matchers with ErgoTestHelpers {
  import org.ergoplatform.utils.ErgoCoreTestConstants._
  import org.ergoplatform.utils.ErgoNodeTestConstants._

  implicit private val timeout: Timeout = defaultTimeout

  private val candidateGenDelay: FiniteDuration    = 3.seconds
  private val blockValidationDelay: FiniteDuration = 2.seconds

  private val defaultSettings: ErgoSettings = {
    val empty = ErgoSettingsReader.read()
    val nodeSettings = empty.nodeSettings.copy(
      mining                       = true,
      stateType                    = StateType.Utxo,
      internalMinerPollingInterval = 1.second,
      offlineGeneration            = true,
      verifyTransactions           = true
    )
    val chainSettings = empty.chainSettings.copy(blockInterval = 1.second)
    empty.copy(
      networkType = DevNet60,
      nodeSettings = nodeSettings,
      chainSettings = chainSettings
    )
  }

  private def withBlockVersion(
    context: ErgoStateContext,
    blockVersion: Byte
  ): ErgoStateContext = {
    val parameters = new Parameters(
      context.currentParameters.height,
      context.currentParameters.parametersTable.updated(
        Parameters.BlockVersion,
        blockVersion.toInt
      ),
      context.currentParameters.proposedUpdate
    )
    new ErgoStateContext(
      context.lastHeaders,
      context.lastExtensionOpt,
      context.genesisStateDigest,
      parameters,
      context.validationSettings,
      context.votingData
    )(context.chainSettings)
  }

  /**
    * Delegate every state operation to the real reader except for the state context
    * under test. This keeps candidate construction on the production path without
    * mutating the live test store.
    */
  private def withStateContext(
    underlying: UtxoStateReader,
    context: ErgoStateContext
  ): UtxoStateReader =
    Proxy
      .newProxyInstance(
        classOf[UtxoStateReader].getClassLoader,
        Array(classOf[UtxoStateReader]),
        new InvocationHandler {
          override def invoke(
            proxy: Any,
            method: Method,
            args: Array[AnyRef]
          ): AnyRef = {
            if (method.getName == "stateContext" && method.getParameterCount == 0) {
              context
            } else {
              val actualArgs = Option(args).getOrElse(Array.empty[AnyRef])
              try {
                method.invoke(underlying, actualArgs: _*)
              } catch {
                case e: InvocationTargetException => throw e.getCause
              }
            }
          }
        }
      )
      .asInstanceOf[UtxoStateReader]

  /**
    * Force the primary proof attempt to fail while delegating the emission-only
    * retry to the real state reader.
    */
  private def failFirstProofAttempt(
    underlying: UtxoStateReader,
    proofAttempts: AtomicInteger
  ): UtxoStateReader =
    Proxy
      .newProxyInstance(
        classOf[UtxoStateReader].getClassLoader,
        Array(classOf[UtxoStateReader]),
        new InvocationHandler {
          override def invoke(
            proxy: Any,
            method: Method,
            args: Array[AnyRef]
          ): AnyRef = {
            if (
              method.getName == "proofsForTransactions" &&
              proofAttempts.getAndIncrement() == 0
            ) {
              Failure(new IllegalStateException("forced primary proof failure"))
            } else {
              val actualArgs = Option(args).getOrElse(Array.empty[AnyRef])
              try {
                method.invoke(underlying, actualArgs: _*)
              } catch {
                case e: InvocationTargetException => throw e.getCause
              }
            }
          }
        }
      )
      .asInstanceOf[UtxoStateReader]

  private def mineAndApplyInitialBlock(
    candidateGenerator: ActorRef,
    testProbe: TestProbe,
    settings: ErgoSettings
  ): Unit = {
    candidateGenerator.tell(
      GenerateCandidate(Seq.empty, reply = true, forced = false),
      testProbe.ref
    )
    val initialCandidate = testProbe.expectMsgPF(candidateGenDelay) {
      case StatusReply.Success(candidate: Candidate) => candidate
    }
    val initialBlock = settings.chainSettings.powScheme
      .proveCandidate(initialCandidate.candidateBlock, defaultMinerSecret.w, 0, 1000)
      .get

    candidateGenerator.tell(initialBlock.header.powSolution, testProbe.ref)
    testProbe.fishForMessage(blockValidationDelay) {
      case StatusReply.Success(()) =>
        testProbe.expectMsgPF(candidateGenDelay) {
          case FullBlockApplied(header) if header.id != initialBlock.header.parentId =>
        }
        true
      case FullBlockApplied(header) if header.id != initialBlock.header.parentId =>
        testProbe.expectMsg(StatusReply.Success(()))
        true
    }
  }

  "The candidate script-version guard" should "enforce the interpreter limit" in {
    val invalidInitialVersion = intercept[IllegalArgumentException] {
      CandidateGenerator.ensureCandidateScriptVersionSupported(
        blockVersion = 0,
        maxSupportedScriptVersion = 3
      )
    }
    invalidInitialVersion.getMessage should include(
      "block version 0 maps to invalid script version -1"
    )

    CandidateGenerator.ensureCandidateScriptVersionSupported(
      blockVersion = 4,
      maxSupportedScriptVersion = 3
    ) shouldBe 4

    CandidateGenerator.ensureCandidateScriptVersionSupported(
      blockVersion = 5,
      maxSupportedScriptVersion = 4
    ) shouldBe 5

    val v5OnV6Interpreter = intercept[IllegalArgumentException] {
      CandidateGenerator.ensureCandidateScriptVersionSupported(
        blockVersion = 5,
        maxSupportedScriptVersion = 3
      )
    }
    v5OnV6Interpreter.getMessage should include(
      "block version 5 requires script version 4, but this interpreter supports up to 3"
    )

    val v6OnV7Interpreter = intercept[IllegalArgumentException] {
      CandidateGenerator.ensureCandidateScriptVersionSupported(
        blockVersion = 6,
        maxSupportedScriptVersion = 4
      )
    }
    v6OnV7Interpreter.getMessage should include(
      "block version 6 requires script version 5, but this interpreter supports up to 4"
    )
  }

  it should "use the linked interpreter maximum by default" in {
    val highestSupportedBlockVersion =
      (VersionContext.MaxSupportedScriptVersion + 1).toByte
    CandidateGenerator.ensureCandidateScriptVersionSupported(
      highestSupportedBlockVersion
    ) shouldBe highestSupportedBlockVersion

    val firstUnsupportedBlockVersion = (highestSupportedBlockVersion + 1).toByte
    val error = intercept[IllegalArgumentException] {
      CandidateGenerator.ensureCandidateScriptVersionSupported(
        firstUnsupportedBlockVersion
      )
    }
    error.getMessage should include(
      s"this interpreter supports up to ${VersionContext.MaxSupportedScriptVersion}"
    )
  }

  it should "stop production generation at the linked interpreter limit" in new TestKit(
    ActorSystem()
  ) {
    val testProbe = new TestProbe(system)
    system.eventStream.subscribe(testProbe.ref, classOf[FullBlockApplied])

    val settings = defaultSettings.copy(
      directory = s"${defaultSettings.directory}-script-version-${System.nanoTime()}"
    )
    val viewHolderRef: ActorRef    = ErgoNodeViewRef(settings)
    val readersHolderRef: ActorRef = ErgoReadersHolderRef(viewHolderRef)
    val candidateGenerator: ActorRef =
      CandidateGenerator(
        defaultMinerSecret.publicImage,
        readersHolderRef,
        viewHolderRef,
        settings
      )

    try {
      mineAndApplyInitialBlock(candidateGenerator, testProbe, settings)

      val readers = await((readersHolderRef ? GetReaders).mapTo[Readers])
      val state   = readers.s.asInstanceOf[UtxoStateReader]
      val supportedBlockVersion =
        (VersionContext.MaxSupportedScriptVersion + 1).toByte
      val supportedContext = withBlockVersion(
        state.stateContext,
        supportedBlockVersion
      )
      val stateWithSupportedVersion = withStateContext(state, supportedContext)

      val supportedResult = CandidateGenerator
        .generateCandidate(
          readers.h,
          stateWithSupportedVersion,
          readers.m,
          defaultMinerSecret.publicImage,
          Seq.empty,
          settings
        )
        .getOrElse(
          fail("The emission transaction should make candidate generation available")
        )

      supportedResult.isSuccess shouldBe true
      supportedResult.get._1.candidateBlock.version shouldBe supportedBlockVersion

      val proofAttempts = new AtomicInteger(0)
      val stateWithFallback = failFirstProofAttempt(
        stateWithSupportedVersion,
        proofAttempts
      )
      val fallbackResult = CandidateGenerator
        .generateCandidate(
          readers.h,
          stateWithFallback,
          readers.m,
          defaultMinerSecret.publicImage,
          Seq.empty,
          settings
        )
        .getOrElse(
          fail("The emission transaction should make fallback generation available")
        )

      fallbackResult.isSuccess shouldBe true
      proofAttempts.get() shouldBe 2
      fallbackResult.get._1.candidateBlock.version shouldBe supportedBlockVersion
      fallbackResult.get._1.candidateBlock.transactions.size shouldBe 1

      val unsupportedBlockVersion = (supportedBlockVersion + 1).toByte
      val unsupportedContext = withBlockVersion(
        state.stateContext,
        unsupportedBlockVersion
      )
      val stateWithUnsupportedVersion = withStateContext(state, unsupportedContext)

      val result = CandidateGenerator
        .generateCandidate(
          readers.h,
          stateWithUnsupportedVersion,
          readers.m,
          defaultMinerSecret.publicImage,
          Seq.empty,
          settings
        )
        .getOrElse(
          fail("The emission transaction should make candidate generation available")
        )

      val error = result.failed.get
      error shouldBe a[IllegalArgumentException]
      error.getMessage should include(
        s"block version $unsupportedBlockVersion requires script version " +
          s"${VersionContext.MaxSupportedScriptVersion + 1}"
      )
      error.getMessage should include(
        s"this interpreter supports up to ${VersionContext.MaxSupportedScriptVersion}"
      )
    } finally {
      system.terminate()
    }
  }
}
