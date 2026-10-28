package org.ergoplatform.nodeView.history.extra

import akka.actor.{ActorRef, Props}
import akka.testkit.TestProbe
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.core.idToVersion
import org.ergoplatform.nodeView.ErgoNodeViewHolder.ReceivableMessages.GetDataFromCurrentView
import org.ergoplatform.nodeView.history.ErgoHistory
import org.ergoplatform.nodeView.state.{ErgoState, StateType}
import org.ergoplatform.nodeView.state.wrapped.WrappedUtxoState
import org.ergoplatform.settings.CacheSettings
import org.ergoplatform.utils.fixtures.NodeViewFixture
import org.ergoplatform.utils.generators.ValidBlocksGenerators.{createUtxoState, validFullBlock}
import org.ergoplatform.utils.{ErgoCorePropertyTest, NodeViewTestConfig, NodeViewTestOps}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Diagnostic of the top-level Holder/indexer ownership join on guarded history storage. */
class HolderIndexerRestartJoinSpec extends ErgoCorePropertyTest with NodeViewTestOps {
  import org.ergoplatform.utils.ErgoCoreTestConstants.parameters

  private case object Inspect
  private case class Inspection(history: ErgoHistory, incarnation: Int)
  private case class RestartedIndexer(cause: Throwable)

  private class ObservedIndexer(cache: CacheSettings, encoder: ErgoAddressEncoder,
                                holder: ActorRef, observer: ActorRef, starts: AtomicInteger,
                                caughtUp: CountDownLatch)
    extends ExtraIndexer(cache, encoder, Some(holder), true) {
    private val incarnation = starts.incrementAndGet()

    override protected def caughtUpHook(height: Int): Unit = caughtUp.countDown()

    override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
      observer ! RestartedIndexer(reason)
      super.preRestart(reason, message)
    }

    override def receive: Receive = super.receive.orElse {
      case Inspect => sender() ! Inspection(_history, incarnation)
    }
  }

  for (restartHolder <- Seq(false, true)) {
    property(s"indexer joins applied blocks after holder restart=$restartHolder") {
      val defaults = NodeViewTestConfig(StateType.Utxo, verifyTransactions = true,
        popowBootstrap = false).toSettings
      val settings = defaults.copy(nodeSettings = defaults.nodeSettings.copy(extraIndex = true))
      new NodeViewFixture(settings, parameters).apply { fixture =>
        import fixture._
        val observer = TestProbe()(actorSystem)
        val query = TestProbe()(actorSystem)
        val watcher = TestProbe()(actorSystem)
        val starts = new AtomicInteger(0)
        val caughtUp = new CountDownLatch(1)
        val holder = nodeViewHolderRef
        var indexerOpt: Option[ActorRef] = None
        val (initialState, boxes) = createUtxoState(fixture.settings)
        try {
          val first = validFullBlock(None, initialState, boxes)
          val afterFirst = WrappedUtxoState(initialState, boxes, fixture.settings)
            .applyModifier(first)(_ => ()).get
          applyBlock(first) shouldBe 'success
          query.awaitAssert(getCurrentState.version shouldBe idToVersion(first.id), 15.seconds, 50.millis)
          val indexer = actorSystem.actorOf(Props(new ObservedIndexer(
            fixture.settings.cacheSettings, fixture.settings.chainSettings.addressEncoder,
            holder, observer.ref, starts, caughtUp)))
          indexerOpt = Some(indexer)
          watcher.watch(indexer)

          def inspection(): Inspection = {
            query.send(indexer, Inspect)
            query.expectMsgType[Inspection](10.seconds)
          }
          def checkpoint(history: ErgoHistory, id: scorex.util.ModifierId): Unit = {
            query.awaitAssert({
              val state = IndexerState.fromHistory(history)
              state.indexedHeaderId shouldBe Some(id)
            }, 15.seconds, 50.millis)
          }
          val oldHistory = getHistory
          checkpoint(oldHistory, first.id)
          caughtUp.await(15, TimeUnit.SECONDS) shouldBe true
          val initial = inspection()
          initial.history should be theSameInstanceAs oldHistory
          initial.incarnation shouldBe 1

          if (restartHolder) {
            query.send(holder, GetDataFromCurrentView[ErgoState[_], Unit] { _ =>
              throw new RuntimeException("controlled holder restart")
            })
            query.send(holder, GetDataFromCurrentView[ErgoState[_], ErgoHistory](_.history))
            val reopened = query.expectMsgType[ErgoHistory](15.seconds)
            reopened should not be theSameInstanceAs(oldHistory)
            val closed = intercept[IllegalStateException] {
              oldHistory.modifierBytesById(first.id)
            }
            closed.getMessage shouldBe "History storage is closed"
            actorSystem.whenTerminated.isCompleted shouldBe false
            val beforeEvent = inspection()
            beforeEvent.history should be theSameInstanceAs oldHistory
            beforeEvent.incarnation shouldBe 1
          }

          val second = validFullBlock(Some(first), afterFirst)
          applyBlock(second) shouldBe 'success
          query.awaitAssert(getCurrentState.version shouldBe idToVersion(second.id), 15.seconds, 50.millis)
          if (restartHolder) {
            val failure = observer.expectMsgType[RestartedIndexer](15.seconds)
            failure.cause shouldBe a[IllegalStateException]
            failure.cause.getMessage shouldBe "History storage is closed"
          }
          val current = getHistory
          checkpoint(current, second.id)
          val finalInspection = inspection()
          finalInspection.history should be theSameInstanceAs current
          finalInspection.incarnation shouldBe (if (restartHolder) 2 else 1)
          starts.get() shouldBe (if (restartHolder) 2 else 1)
          actorSystem.whenTerminated.isCompleted shouldBe false
        } finally {
          try {
            indexerOpt.foreach { indexer =>
              actorSystem.stop(indexer)
              watcher.expectTerminated(indexer, 15.seconds)
            }
          } finally {
            initialState.closeStorage()
          }
        }
      }
    }
  }
}
