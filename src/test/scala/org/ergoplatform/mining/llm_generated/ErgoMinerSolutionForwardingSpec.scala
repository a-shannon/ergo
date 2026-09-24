package org.ergoplatform.mining

import akka.actor.{ActorSystem, Props}
import akka.pattern.StatusReply
import akka.testkit.{TestKit, TestProbe}
import org.ergoplatform.{AutolykosSolution, InputSolutionFound, OrderingSolutionFound, SolutionFound}
import org.ergoplatform.mining.ErgoMiner.MinerState
import org.ergoplatform.settings.ErgoSettingsReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import sigma.data.ProveDlog

import scala.concurrent.duration._

class ErgoMinerSolutionForwardingSpec
  extends TestKit(ActorSystem("ErgoMinerSolutionForwardingSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private val settings = ErgoSettingsReader.read()
  private val publicKey = ProveDlog(genPk(BigInt(1)))
  private val solution = new AutolykosSolution(
    publicKey.value,
    AutolykosSolution.wForV2,
    Array.fill[Byte](8)(0),
    AutolykosSolution.dForV2
  )

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def assertForwarded(message: SolutionFound): Unit = {
    val candidateGenerator = TestProbe()
    val requester = TestProbe()
    val unusedReader = TestProbe()
    val miner = system.actorOf(Props(new ErgoMiner(
      settings,
      unusedReader.ref,
      unusedReader.ref,
      None
    ) {
      // Exercise the production started receive without booting a wallet or miner.
      override def preStart(): Unit =
        context.become(started(MinerState(None, publicKey, candidateGenerator.ref)))
    }))

    try {
      requester.send(miner, message)
      candidateGenerator.expectMsg(3.seconds, message)
      candidateGenerator.lastSender shouldBe requester.ref
      val accepted = StatusReply.success(())
      candidateGenerator.reply(accepted)
      requester.expectMsg(3.seconds, accepted)
    } finally {
      system.stop(miner)
    }
  }

  "ErgoMiner" should "forward an ordering solution with its original sender and reply" in {
    assertForwarded(OrderingSolutionFound(solution))
  }

  it should "forward an input solution with its original sender and reply" in {
    assertForwarded(InputSolutionFound(solution))
  }

  it should "reject both typed solutions before mining starts" in {
    val candidateGenerator = TestProbe()
    val requester = TestProbe()
    val unusedReader = TestProbe()
    val miner = system.actorOf(Props(new ErgoMiner(
      settings, unusedReader.ref, unusedReader.ref, None
    ) {
      override def preStart(): Unit =
        context.become(starting(MinerState(None, publicKey, candidateGenerator.ref)))
    }))
    try {
      Seq(InputSolutionFound(solution), OrderingSolutionFound(solution)).foreach { message =>
        requester.send(miner, message)
        requester.expectMsgPF(3.seconds) {
          case StatusReply.Error(error) => error.getMessage shouldBe "Miner has not started yet"
        }
      }
      candidateGenerator.expectNoMessage(100.millis)
    } finally system.stop(miner)
  }
}
