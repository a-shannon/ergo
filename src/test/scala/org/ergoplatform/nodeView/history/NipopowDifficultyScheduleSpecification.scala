package org.ergoplatform.nodeView.history

import io.circe.{Decoder, HCursor}
import org.ergoplatform.local.NipopowVerifier
import org.ergoplatform.mining.AutolykosPowScheme
import org.ergoplatform.modifiers.history.header.{Header, HeaderSerializer}
import org.ergoplatform.modifiers.history.popow.{
  NipopowAlgos,
  NipopowProof,
  NipopowProofSerializer
}
import org.ergoplatform.network.message.NipopowProofSpec
import org.ergoplatform.nodeView.state.StateType
import org.ergoplatform.settings.NipopowSettings
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.wallet.utils.FileUtils
import scorex.util.ModifierId
import scorex.util.encode.Base16

import java.security.MessageDigest
import scala.io.Source

class NipopowDifficultyScheduleSpecification extends ErgoCorePropertyTest with FileUtils {
  import org.ergoplatform.utils.ErgoNodeTestConstants.{settings => baseSettings}

  private val FixtureResource = "nipopow-difficulty-schedule-pair-v1.json"
  private val FixtureFormat   = "ergo-nipopow-difficulty-schedule-pair-v1"

  private val pow = new AutolykosPowScheme(
    baseSettings.chainSettings.powScheme.k,
    baseSettings.chainSettings.powScheme.n
  )

  private val chainSettings = baseSettings.chainSettings.copy(
    powScheme            = pow,
    initialDifficultyHex = "64",
    epochLength          = 10000,
    useLastEpochs        = 3
  )
  private val algos      = new NipopowAlgos(chainSettings)
  private val serializer = new NipopowProofSerializer(algos)

  private lazy val fixture: HCursor = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(FixtureResource))
      .getOrElse(
        throw new IllegalArgumentException(s"Missing resource: $FixtureResource")
      )
    val source = Source.fromInputStream(stream, "UTF-8")
    val text =
      try source.mkString
      finally source.close()
    io.circe.parser.parse(text).fold(error => throw error, _.hcursor)
  }

  private def fixtureValue[A: Decoder](field: String): A =
    fixture.get[A](field).fold(error => throw error, identity)

  private def proofValue[A: Decoder](proofName: String, field: String): A =
    fixture.downField(proofName).get[A](field).fold(error => throw error, identity)

  private def proofBytes(proofName: String): Array[Byte] =
    Base16.decode(proofValue[String](proofName, "bytes_hex")).get

  private lazy val honestBytes        = proofBytes("honest")
  private lazy val lowDifficultyBytes = proofBytes("low_difficulty")
  private lazy val honestProof        = serializer.parseBytes(honestBytes)
  private lazy val lowDifficultyProof = serializer.parseBytes(lowDifficultyBytes)

  private def sourceHeaders(proofName: String): Seq[Header] =
    proofValue[Seq[String]](proofName, "source_headers_hex")
      .map(hex => HeaderSerializer.parseBytes(Base16.decode(hex).get))

  private lazy val honestSourceHeaders        = sourceHeaders("honest")
  private lazy val lowDifficultySourceHeaders = sourceHeaders("low_difficulty")
  private lazy val firstLowDifficultyHeader   = lowDifficultySourceHeaders(1)

  private def sha256(bytes: Array[Byte]): String =
    Base16.encode(MessageDigest.getInstance("SHA-256").digest(bytes))

  private def freshHistory(nipopowBootstrap: Boolean): ErgoHistory = {
    val settings = baseSettings.copy(
      directory = createTempDir.getAbsolutePath,
      chainSettings =
        chainSettings.copy(genesisId = Some(honestProof.headersChain.head.id)),
      nodeSettings = baseSettings.nodeSettings.copy(
        stateType          = StateType.Utxo,
        verifyTransactions = true,
        blocksToKeep       = -1,
        nipopowSettings    = NipopowSettings(nipopowBootstrap, p2pNipopows = 2)
      )
    )
    ErgoHistory.readOrGenerate(settings)(null)
  }

  private def finalTip(first: NipopowProof, second: NipopowProof): ModifierId = {
    val verifier = new NipopowVerifier(Some(honestProof.headersChain.head.id))
    verifier.process(first)
    verifier.process(second)
    verifier.bestChain.last.id
  }

  property("the frozen proof pair preserves its serialization, PoW, and score evidence") {
    fixtureValue[String]("format") shouldBe FixtureFormat
    fixtureValue[String]("source_commit") shouldBe
    "57d7f04c80544d84c39c0a4c31f8e8f5817a9f50"
    fixtureValue[Int]("m") shouldBe ErgoHistoryUtils.P2PNipopowProofM
    fixtureValue[Int]("k") shouldBe ErgoHistoryUtils.P2PNipopowProofK
    fixtureValue[Boolean]("continuous") shouldBe true
    fixtureValue[String]("genesis_id") shouldBe honestProof.headersChain.head.id
    fixtureValue[Int]("initial_difficulty") shouldBe chainSettings.initialDifficulty.toInt
    fixtureValue[Int]("attacker_difficulty") shouldBe 1

    Seq(
      ("honest", honestBytes, honestProof, honestSourceHeaders),
      ("low_difficulty", lowDifficultyBytes, lowDifficultyProof,
        lowDifficultySourceHeaders)
    )
      .foreach {
        case (proofName, bytes, proof, source) =>
          bytes.length shouldBe proofValue[Int](proofName, "proof_size")
          sha256(bytes) shouldBe proofValue[String](proofName, "sha256")

          val messagePayload = NipopowProofSpec.toBytes(bytes)
          messagePayload.length shouldBe proofValue[Int](proofName, "payload_size")
          messagePayload.length should be <= NipopowProofSpec.SizeLimit
          NipopowProofSpec.parseBytes(messagePayload) shouldBe bytes

          serializer.toBytes(proof) shouldBe bytes
          proof.headersChain.size shouldBe
            proofValue[Int](proofName, "proof_header_count")
          proof.m shouldBe ErgoHistoryUtils.P2PNipopowProofM
          proof.k shouldBe ErgoHistoryUtils.P2PNipopowProofK
          proof.continuous shouldBe true
          proof.suffixTail.size shouldBe ErgoHistoryUtils.P2PNipopowProofK - 1
          proof.headersChain.head.id shouldBe honestProof.headersChain.head.id
          proof.hasValidParams shouldBe true
          proof.hasValidConnections shouldBe true
          proof.hasValidHeights shouldBe true
          proof.hasValidDifficultyHeaders shouldBe true
          proof.hasValidPow shouldBe true
          proof.isValid shouldBe true
          proof.prefix.forall(_.checkInterlinksProof()) shouldBe true
          proof.suffixHead.checkInterlinksProof() shouldBe true
          proof.headersChain.tail.map(_.requiredDifficulty).sum.toString shouldBe
            proofValue[String](proofName, "proof_declared_work")
          algos.bestArg(proof.headersChain.tail)(proof.m) shouldBe
            proofValue[Int](proofName, "proof_score")

          source.size shouldBe proofValue[Int](proofName, "source_chain_length")
          source.map(_.id).distinct.size shouldBe source.size
          source.map(h => Base16.encode(HeaderSerializer.toBytes(h))) shouldBe
            proofValue[Seq[String]](proofName, "source_headers_hex")
          source.foreach(header => pow.validate(header).isSuccess shouldBe true)
          source.zip(source.tail).foreach {
            case (parent, child) =>
              child.parentId shouldBe parent.id
              child.height shouldBe parent.height + 1
          }
          source.last.id shouldBe proof.headersChain.last.id
          val sourceBytesById = source.map { header =>
            header.id -> HeaderSerializer.toBytes(header).toSeq
          }.toMap
          proof.headersChain.foreach { header =>
            sourceBytesById.get(header.id) shouldBe
              Some(HeaderSerializer.toBytes(header).toSeq)
          }
          source.tail.map(_.requiredDifficulty).sum.toString shouldBe
            proofValue[String](proofName, "source_declared_work")
      }

    honestProof.headersChain.last.id shouldBe proofValue[String]("honest", "tip_id")
    lowDifficultyProof.headersChain.last.id shouldBe
      proofValue[String]("low_difficulty", "tip_id")
    honestSourceHeaders.head.id shouldBe fixtureValue[String]("genesis_id")
    lowDifficultySourceHeaders.head.id shouldBe fixtureValue[String]("genesis_id")
    algos
      .lowestCommonAncestor(honestProof.headersChain, lowDifficultyProof.headersChain)
      .map(_.id) shouldBe Some(fixtureValue[String]("genesis_id"))
    honestSourceHeaders.tail.map(_.requiredDifficulty).distinct shouldBe Seq(BigInt(100))
    lowDifficultySourceHeaders.tail
      .map(_.requiredDifficulty)
      .distinct shouldBe Seq(BigInt(1))
    lowDifficultySourceHeaders.forall(_.height < chainSettings.epochLength) shouldBe true
    honestSourceHeaders.tail.map(_.requiredDifficulty).sum should be >
      lowDifficultySourceHeaders.tail.map(_.requiredDifficulty).sum
  }

  property(
    "ordinary admission accepts the honest source and rejects the low-difficulty fork"
  ) {
    val honestHistory = freshHistory(nipopowBootstrap = false)
    try {
      honestSourceHeaders.foreach { header =>
        withClue(s"ordinary honest admission at height ${header.height}: ") {
          honestHistory.applicableTry(header).isSuccess shouldBe true
          honestHistory.append(header).isSuccess shouldBe true
        }
      }
      honestHistory.bestHeaderOpt.map(_.id) shouldBe honestSourceHeaders.lastOption.map(
        _.id
      )
    } finally {
      honestHistory.closeStorage()
    }

    val lowDifficultyHistory = freshHistory(nipopowBootstrap = false)
    try {
      val genesis = honestSourceHeaders.head
      lowDifficultyHistory.append(genesis).isSuccess shouldBe true
      firstLowDifficultyHeader.parentId shouldBe genesis.id
      firstLowDifficultyHeader.requiredDifficulty shouldBe BigInt(1)
      pow.validate(firstLowDifficultyHeader).isSuccess shouldBe true
      lowDifficultyHistory.requiredDifficultyAfter(genesis) shouldBe BigInt(100)

      val rejection = lowDifficultyHistory.applicableTry(firstLowDifficultyHeader)
      rejection.isFailure shouldBe true
      rejection.failed.get.getMessage should include(
        "A header should contain correct required difficulty"
      )
      rejection.failed.get.getMessage should include("Given: 1, expected 100")
      lowDifficultyHistory.append(firstLowDifficultyHeader).isFailure shouldBe true
    } finally {
      lowDifficultyHistory.closeStorage()
    }
  }

  property("an honest proof stays selected regardless of proof arrival order") {
    val observedTips = Seq(
      finalTip(honestProof, lowDifficultyProof),
      finalTip(lowDifficultyProof, honestProof)
    )

    observedTips shouldBe Seq.fill(2)(honestProof.headersChain.last.id)
  }

  property(
    "bootstrap excludes schedule-invalid headers and converges regardless of " +
      "arrival order"
  ) {
    val honestIds  = honestProof.headersChain.map(_.id).toSet
    val lowOnlyIds = lowDifficultyProof.headersChain.map(_.id).toSet -- honestIds
    lowOnlyIds should not be empty

    def bootstrapOutcome(
      first: NipopowProof,
      second: NipopowProof
    ): (Boolean, Option[ModifierId]) = {
      val history = freshHistory(nipopowBootstrap = true)
      try {
        history.applyPopowProof(first)
        history.applyPopowProof(second)
        history.applyPopowProof(honestProof)
        lowOnlyIds.exists(history.contains) -> history.bestHeaderOpt.map(_.id)
      } finally {
        history.closeStorage()
      }
    }

    val outcomes = Seq(
      bootstrapOutcome(honestProof, lowDifficultyProof),
      bootstrapOutcome(lowDifficultyProof, honestProof)
    )
    outcomes shouldBe Seq.fill(2)(
      false -> Some(honestProof.headersChain.last.id)
    )
  }
}
