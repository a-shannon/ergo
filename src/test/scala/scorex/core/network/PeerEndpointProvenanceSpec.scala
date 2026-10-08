package scorex.core.network

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.ByteString
import org.ergoplatform.db.DBSpec
import org.ergoplatform.network.ErgoNodeViewSynchronizerMessages.{DisconnectedPeer, HandshakedPeer}
import org.ergoplatform.network.message.{GetPeersSpec, Message, PeersSpec}
import org.ergoplatform.network.message.MessageConstants.MessageCode
import org.ergoplatform.network.peer.{LocalAddressPeerFeature, PeerDatabase, PeerInfo, PeerManagerRef}
import org.ergoplatform.network.{Handshake, HandshakeSerializer, PeerSpec, PeerSpecSerializer}
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.ErgoNodeTestConstants.{defaultPeerSpec, settings}
import scorex.db.LDBFactory
import scorex.core.app.ScorexContext
import scorex.testkit.utils.AkkaFixture
import scorex.util.serialization.VLQByteStringWriter

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.net.InetSocketAddress
import scala.concurrent.Await
import scala.concurrent.duration._

class PeerEndpointProvenanceSpec extends ErgoCorePropertyTest with DBSpec {

  import org.ergoplatform.network.peer.PeerManager.ReceivableMessages.{GetAllPeers, SeenPeers}
  import scorex.core.network.NetworkController.ReceivableMessages.{ConnectTo, SendToNetwork}

  private val claimed = new InetSocketAddress("198.51.100.20", 9030)
  private val connected = new InetSocketAddress("203.0.113.21", 9031)
  private val dialed = new InetSocketAddress("192.0.2.22", 9032)
  private val otherClaim = new InetSocketAddress("192.0.2.23", 9033)
  private val localClaim = new InetSocketAddress("192.0.2.24", 9034)

  private class Fixture(maxKnownPeers: Int = 2,
                        allowLocal: Boolean = settings.scorexSettings.network.allowLocal,
                        bindAddress: InetSocketAddress = settings.scorexSettings.network.bindAddress,
                        persistedPeers: Seq[PeerInfo] = Seq.empty,
                        legacyPeers: Seq[PeerInfo] = Seq.empty,
                        externalNodeAddress: Option[InetSocketAddress] = None) extends AkkaFixture {
    implicit val ec = system.dispatcher
    implicit val actorSystem: ActorSystem = system

    val directory: File = createTempDir
    val config = settings.copy(
      directory = directory.getAbsolutePath,
      scorexSettings = settings.scorexSettings.copy(
        network = settings.scorexSettings.network.copy(
          knownPeers = Seq(connected),
          allowLocal = allowLocal,
          bindAddress = bindAddress
        )
      )
    )
    val oldHandshake = System.currentTimeMillis() - PeerDatabase.KnownPeerMaxAgeMs - 1000L
    private val initialDb = new PeerDatabase(config, maxKnownPeers)
    initialDb.addOrUpdateKnownPeer(PeerInfo.fromAddress(claimed).copy(lastHandshake = oldHandshake))
    initialDb.addOrUpdateKnownPeer(PeerInfo.fromAddress(connected))
    persistedPeers.foreach(info => initialDb.addOrUpdateKnownPeer(info))
    initialDb.close()
    if (legacyPeers.nonEmpty) {
      val rawStore = LDBFactory.createKvDb(s"${directory.getAbsolutePath}/peers")
      try {
        legacyPeers.foreach { info =>
          val address = info.peerSpec.address.get
          val keyStream = new ByteArrayOutputStream()
          val objectStream = new ObjectOutputStream(keyStream)
          objectStream.writeObject(address)
          objectStream.close()
          val oldWriter = new VLQByteStringWriter
          oldWriter.putLong(info.lastHandshake)
          oldWriter.putOption(info.connectionType)((writer, direction) =>
            writer.putBoolean(direction.isIncoming))
          PeerSpecSerializer.serialize(info.peerSpec, oldWriter)
          rawStore.insert(keyStream.toByteArray, oldWriter.toBytes)
        }
      } finally rawStore.close()
    }

    val context = ScorexContext(Seq.empty, None, externalNodeAddress)
    val peerManager: ActorRef = system.actorOf(PeerManagerRef.props(config, context, maxKnownPeers))
    val tcpManager = TestProbe("TcpManager")
    val events = TestProbe("PeerEvents")
    system.eventStream.subscribe(events.ref, classOf[HandshakedPeer])
    val lifecycle = TestProbe("PeerLifecycle")
    system.eventStream.subscribe(lifecycle.ref, classOf[HandshakedPeer])
    system.eventStream.subscribe(lifecycle.ref, classOf[DisconnectedPeer])
    val forwardedMessages = TestProbe("ForwardedMessages")

    val controller: TestActorRef[NetworkController] = TestActorRef(new NetworkController(
      config,
      peerManager,
      context,
      tcpManager.ref,
      _ => Map[MessageCode, ActorRef](GetPeersSpec.messageCode -> forwardedMessages.ref)
    ))
    tcpManager.expectMsgType[Tcp.Bind]

    def allPeers: Map[InetSocketAddress, PeerInfo] = {
      val query = TestProbe()
      query.send(peerManager, GetAllPeers)
      query.expectMsgType[Map[InetSocketAddress, PeerInfo]]
    }

    def connectedPeers: Iterable[ConnectedPeer] = {
      val query = TestProbe()
      query.send(controller, NetworkController.ReceivableMessages.GetConnectedPeers)
      query.expectMsgType[Iterable[ConnectedPeer]]
    }

    private def receiveHandshake(connection: TestProbe, advertised: PeerSpec): (HandshakedPeer, Handshake) = {
      val handler = connection.expectMsgType[Tcp.Register].handler
      connection.expectMsg(Tcp.ResumeReading)
      val ownHandshake = HandshakeSerializer.parseBytesTry(connection.expectMsgType[Tcp.Write].data.toArray).get
      val bytes = HandshakeSerializer.toBytes(Handshake(advertised, System.currentTimeMillis()))
      connection.send(handler, Tcp.Received(ByteString(bytes)))
      connection.expectMsg(Tcp.ResumeReading)
      val event = events.expectMsgType[HandshakedPeer]
      // The event stream and direct manager messages have different senders.
      val barrier = TestProbe()
      barrier.send(peerManager, event)
      barrier.send(peerManager, GetAllPeers)
      barrier.expectMsgType[Map[InetSocketAddress, PeerInfo]]
      event -> ownHandshake
    }

    def incoming(remote: InetSocketAddress, advertised: PeerSpec): HandshakedPeer = {
      incomingAtLocal(remote, config.scorexSettings.network.bindAddress, advertised)
    }

    def incomingAtLocal(remote: InetSocketAddress,
                        local: InetSocketAddress,
                        advertised: PeerSpec): HandshakedPeer = {
      val connection = TestProbe()
      connection.send(controller, Tcp.Connected(remote, local))
      receiveHandshake(connection, advertised)._1
    }

    def beginDial(address: InetSocketAddress): Unit = {
      controller ! ConnectTo(PeerInfo.fromAddress(address))
      tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe address
    }

    def completedDial(address: InetSocketAddress, advertised: PeerSpec): HandshakedPeer = {
      val local = new InetSocketAddress("192.0.2.77", 55010)
      completedDialAtLocal(address, local, advertised)._1
    }

    def completedDialAtLocal(address: InetSocketAddress,
                             local: InetSocketAddress,
                             advertised: PeerSpec): (HandshakedPeer, Handshake) = {
      val connection = TestProbe()
      connection.send(controller, Tcp.Connected(address, local))
      receiveHandshake(connection, advertised)
    }

    def incomingWithLocal(remote: InetSocketAddress,
                          local: InetSocketAddress,
                          advertised: PeerSpec): Handshake = {
      val connection = TestProbe()
      connection.send(controller, Tcp.Connected(remote, local))
      receiveHandshake(connection, advertised)._2
    }

    def outgoing(address: InetSocketAddress, advertised: PeerSpec): HandshakedPeer = {
      beginDial(address)
      completedDial(address, advertised)
    }
  }

  property("an inbound claim cannot own a full table row or suppress its dial") {
    val f = new Fixture
    try {
      f.outgoing(connected, defaultPeerSpec.copy(declaredAddress = Some(connected)))
      f.allPeers(connected).lastHandshake should be > 0L

      // The source IP matches the claimed host, but its source port does not
      // establish ownership of the claimed listening port.
      val inboundSource = new InetSocketAddress(claimed.getAddress, 55000)
      f.incoming(inboundSource, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      f.allPeers(claimed).lastHandshake shouldBe f.oldHandshake

      // The normal selection loop must still reach the claimed row. A direct
      // ConnectTo would miss the exclusion bug in RandomPeerExcluding.
      f.tcpManager.send(f.controller, Tcp.Bound(f.config.scorexSettings.network.bindAddress))
      f.tcpManager.expectMsgType[Tcp.Connect](8.seconds).remoteAddress shouldBe claimed

      val redirected = defaultPeerSpec.copy(
        declaredAddress = Some(otherClaim),
        features = Seq(LocalAddressPeerFeature(localClaim))
      )
      f.outgoing(dialed, redirected)
      val stored = f.allPeers
      stored.keySet shouldBe Set(connected, dialed)
      stored(dialed).peerSpec.address shouldBe Some(dialed)
      stored(dialed).peerSpec.localAddressOpt shouldBe None
      stored(dialed).connectionType shouldBe Some(Outgoing)
      stored(dialed).lastHandshake should be > 0L
      stored(dialed).verifiedOutboundEndpoint shouldBe true
      f.connectedPeers.size shouldBe 3
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("an inbound address is only a dial candidate until an outbound handshake") {
    val f = new Fixture(maxKnownPeers = 3)
    try {
      val candidateAddress = new InetSocketAddress("192.0.2.25", 9035)
      val inboundSource = new InetSocketAddress("198.51.100.40", 55001)
      val advertised = defaultPeerSpec.copy(
        declaredAddress = Some(candidateAddress),
        features = Seq(LocalAddressPeerFeature(localClaim))
      )
      f.incoming(inboundSource, advertised)

      val candidate = f.allPeers(candidateAddress)
      candidate.lastHandshake shouldBe 0L
      candidate.connectionType shouldBe None
      candidate.peerSpec.declaredAddress shouldBe Some(candidateAddress)
      candidate.peerSpec.localAddressOpt shouldBe None
      candidate.verifiedOutboundEndpoint shouldBe false
      f.allPeers(claimed).lastHandshake shouldBe f.oldHandshake

      val query = TestProbe()(f.system)
      query.send(f.peerManager, SeenPeers(10))
      query.expectMsgType[Seq[PeerInfo]].map(_.peerSpec.address) should not contain Some(candidateAddress)

      f.outgoing(candidateAddress, defaultPeerSpec.copy(declaredAddress = Some(candidateAddress)))
      val verified = f.allPeers(candidateAddress)
      verified.lastHandshake should be > 0L
      verified.connectionType shouldBe Some(Outgoing)
      verified.peerSpec.address shouldBe Some(candidateAddress)
      verified.verifiedOutboundEndpoint shouldBe true
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("reloaded legacy rows stay dialable but do not reach peer gossip") {
    val historicalHandshake = System.currentTimeMillis() - 24.hours.toMillis
    val legacyInboundAddress = new InetSocketAddress("8.8.8.61", 9061)
    val legacyOutboundAddress = new InetSocketAddress("8.8.8.62", 9062)
    val legacyInbound = PeerInfo(
      defaultPeerSpec.copy(declaredAddress = Some(legacyInboundAddress)),
      historicalHandshake,
      Some(Incoming)
    )
    val legacyOutbound = PeerInfo(
      defaultPeerSpec.copy(declaredAddress = Some(legacyOutboundAddress)),
      historicalHandshake,
      Some(Outgoing)
    )
    val f = new Fixture(maxKnownPeers = 5, persistedPeers = Seq(legacyInbound, legacyOutbound))
    try {
      // The manager opened the same store after the seed database was closed.
      val reopened = f.allPeers
      reopened(legacyInboundAddress).connectionType shouldBe Some(Incoming)
      reopened(legacyOutboundAddress).connectionType shouldBe Some(Outgoing)
      reopened(legacyInboundAddress).lastHandshake shouldBe historicalHandshake
      reopened(legacyOutboundAddress).lastHandshake shouldBe historicalHandshake
      reopened(legacyInboundAddress).verifiedOutboundEndpoint shouldBe false
      reopened(legacyOutboundAddress).verifiedOutboundEndpoint shouldBe false

      val selection = org.ergoplatform.network.peer.PeerManager.ReceivableMessages
        .RandomPeerExcluding(Seq.empty).choose(
          reopened.filter { case (address, _) =>
            address == legacyInboundAddress || address == legacyOutboundAddress
          },
          Seq.empty,
          f.context
        )
      selection.flatMap(_.peerSpec.declaredAddress).get should (
        be(legacyInboundAddress) or be(legacyOutboundAddress)
      )

      val inboundSource = new InetSocketAddress(connected.getAddress, 55013)
      val inbound = f.incoming(
        inboundSource,
        defaultPeerSpec.copy(declaredAddress = Some(connected))
      )
      inbound.remote.connectionId.direction shouldBe Incoming

      val query = TestProbe()(f.system)
      query.send(f.peerManager, SeenPeers(10))
      val chosen = query.expectMsgType[Seq[PeerInfo]].flatMap(_.peerSpec.declaredAddress).toSet
      chosen should not contain legacyInboundAddress
      chosen should not contain legacyOutboundAddress

      val network = TestProbe()(f.system)
      val synchronizer = f.system.actorOf(
        PeerSynchronizerRef.props(network.ref, f.peerManager, f.config.scorexSettings.network)(f.ec)
      )
      synchronizer ! Message[Unit](GetPeersSpec, Left(Array.empty[Byte]), Some(inbound.remote))
      val sent = network.fishForMessage(5.seconds) {
        case SendToNetwork(message, _) if message.spec.messageCode == PeersSpec.messageCode => true
        case _ => false
      }.asInstanceOf[SendToNetwork]
      // This observes the synchronizer output, not TCP delivery or wire serialization.
      sent.sendingStrategy shouldBe SendToPeer(inbound.remote)
      val shared = sent.message.input.right.get.asInstanceOf[Seq[PeerSpec]]
        .flatMap(_.declaredAddress).toSet
      shared should not contain legacyInboundAddress
      shared should not contain legacyOutboundAddress

      // The controller-accepted outbound handshake is the positive control after legacy selection.
      val accepted = f.outgoing(dialed, defaultPeerSpec.copy(declaredAddress = Some(dialed)))
      accepted.remote.connectionId.direction shouldBe Outgoing
      f.allPeers(dialed).connectionType shouldBe Some(Outgoing)
      f.allPeers(dialed).lastHandshake should be > historicalHandshake
      f.allPeers(dialed).verifiedOutboundEndpoint shouldBe true
      query.send(f.peerManager, SeenPeers(10))
      query.expectMsgType[Seq[PeerInfo]].flatMap(_.peerSpec.declaredAddress) should contain(dialed)
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a legacy local-address feature cannot redirect the outbound dial after reopen") {
    val historicalHandshake = System.currentTimeMillis() - 24.hours.toMillis
    val declared = new InetSocketAddress("198.51.100.80", 9080)
    val redirected = new InetSocketAddress("203.0.113.81", 9081)
    val plain = new InetSocketAddress("198.51.100.82", 9082)
    val legacyRedirect = PeerInfo(
      defaultPeerSpec.copy(
        declaredAddress = Some(declared),
        features = Seq(LocalAddressPeerFeature(redirected))
      ), historicalHandshake, Some(Outgoing)
    )
    val legacyPlain = PeerInfo(
      defaultPeerSpec.copy(declaredAddress = Some(plain)),
      historicalHandshake, Some(Outgoing)
    )
    val f = new Fixture(maxKnownPeers = 5, legacyPeers = Seq(legacyRedirect, legacyPlain))
    try {
      val reopened = f.allPeers
      reopened(declared).verifiedOutboundEndpoint shouldBe false
      reopened(declared).peerSpec.localAddressOpt shouldBe Some(redirected)
      reopened(plain).verifiedOutboundEndpoint shouldBe false

      val selector = org.ergoplatform.network.peer.PeerManager.ReceivableMessages
        .RandomPeerExcluding(Seq.empty)
      val plainChoice = selector.choose(reopened.filter(_._1 == plain), Seq.empty, f.context).get
      f.controller ! ConnectTo(plainChoice)
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe plain

      val redirectedChoice = selector.choose(reopened.filter(_._1 == declared), Seq.empty, f.context).get
      f.controller ! ConnectTo(redirectedChoice)
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe declared
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a legacy LAN-only row remains dialable when local peers are enabled") {
    val local = new InetSocketAddress("192.168.44.83", 9083)
    val legacy = PeerInfo(
      defaultPeerSpec.copy(
        declaredAddress = None,
        features = Seq(LocalAddressPeerFeature(local))
      ), System.currentTimeMillis() - 24.hours.toMillis, Some(Outgoing)
    )
    val f = new Fixture(maxKnownPeers = 3, allowLocal = true, legacyPeers = Seq(legacy))
    try {
      val reopened = f.allPeers(local)
      reopened.verifiedOutboundEndpoint shouldBe false
      f.controller ! ConnectTo(reopened)
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe local
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("an unverified same-NAT claim cannot dial LAN but an explicit LAN dial can") {
    val ownExternal = new InetSocketAddress("198.51.100.90", 9030)
    val peerExternal = new InetSocketAddress(ownExternal.getAddress, 9084)
    val peerLocal = new InetSocketAddress("192.168.44.84", 9084)
    val legacy = PeerInfo(
      defaultPeerSpec.copy(
        declaredAddress = Some(peerExternal),
        features = Seq(LocalAddressPeerFeature(peerLocal))
      ), System.currentTimeMillis() - 24.hours.toMillis, Some(Outgoing)
    )
    val f = new Fixture(
      maxKnownPeers = 5,
      allowLocal = true,
      legacyPeers = Seq(legacy),
      externalNodeAddress = Some(ownExternal)
    )
    try {
      val reopened = f.allPeers(peerExternal)
      reopened.verifiedOutboundEndpoint shouldBe false
      f.controller ! ConnectTo(reopened)
      f.tcpManager.expectNoMessage(500.millis)

      // The operator may explicitly dial a known LAN endpoint. Merely adding
      // it to knownPeers after this nonempty database is reopened does not seed it.
      f.controller ! ConnectTo(PeerInfo.fromAddress(peerLocal))
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe peerLocal
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a WAN peer cannot relay private local-address dial candidates") {
    val public = new InetSocketAddress("198.51.100.93", 9093)
    val localOnly = new InetSocketAddress("192.168.44.93", 9093)
    val pairedLocal = new InetSocketAddress("192.168.44.94", 9094)
    val privateDeclared = new InetSocketAddress("192.168.44.95", 9095)
    val f = new Fixture(maxKnownPeers = 8, allowLocal = true)
    try {
      val source = f.incoming(
        new InetSocketAddress("203.0.113.92", 5592),
        defaultPeerSpec.copy(declaredAddress = None)
      )
      val relayed = Seq(
        defaultPeerSpec.copy(
          declaredAddress = None,
          features = Seq(LocalAddressPeerFeature(localOnly))
        ),
        defaultPeerSpec.copy(
          declaredAddress = Some(public),
          features = Seq(LocalAddressPeerFeature(pairedLocal))
        ),
        defaultPeerSpec.copy(
          declaredAddress = Some(privateDeclared),
          features = Seq.empty
        )
      )
      val network = TestProbe()(f.system)
      val synchronizer = f.system.actorOf(
        PeerSynchronizerRef.props(network.ref, f.peerManager, f.config.scorexSettings.network)(f.ec)
      )
      val spec = new PeersSpec(f.config.scorexSettings.network.maxPeerSpecObjects)
      synchronizer ! Message(spec, Left(spec.toBytes(relayed)), Some(source.remote))

      val poll = TestProbe()(f.system)
      poll.awaitAssert(f.allPeers.keySet should contain(public))
      f.allPeers.keySet should not contain localOnly
      f.allPeers.keySet should not contain pairedLocal
      f.allPeers.keySet should not contain privateDeclared
      f.allPeers(public).peerSpec.localAddressOpt shouldBe None
      f.controller ! ConnectTo(f.allPeers(public))
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe public
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a LAN peer can relay separate public and LAN dial candidates") {
    val sourceAddress = new InetSocketAddress("192.168.44.96", 5596)
    val public = new InetSocketAddress("198.51.100.96", 9096)
    val pairedLocal = new InetSocketAddress(sourceAddress.getAddress, 9097)
    val localOnly = new InetSocketAddress(sourceAddress.getAddress, 9098)
    val thirdPartyLocal = new InetSocketAddress("192.168.44.97", 9099)
    val f = new Fixture(maxKnownPeers = 8, allowLocal = true)
    try {
      val source = f.incoming(sourceAddress, defaultPeerSpec.copy(declaredAddress = None))
      val relayed = Seq(
        defaultPeerSpec.copy(
          declaredAddress = Some(public),
          features = Seq(LocalAddressPeerFeature(pairedLocal))
        ),
        defaultPeerSpec.copy(
          declaredAddress = None,
          features = Seq(LocalAddressPeerFeature(localOnly))
        ),
        defaultPeerSpec.copy(
          declaredAddress = Some(thirdPartyLocal),
          features = Seq.empty
        )
      )
      val network = TestProbe()(f.system)
      val synchronizer = f.system.actorOf(
        PeerSynchronizerRef.props(network.ref, f.peerManager, f.config.scorexSettings.network)(f.ec)
      )
      val spec = new PeersSpec(f.config.scorexSettings.network.maxPeerSpecObjects)
      synchronizer ! Message(spec, Left(spec.toBytes(relayed)), Some(source.remote))

      val poll = TestProbe()(f.system)
      poll.awaitAssert(f.allPeers.keySet should contain(localOnly))
      poll.awaitAssert(f.allPeers.keySet should contain(pairedLocal))
      f.allPeers.keySet should contain(public)
      f.allPeers.keySet should not contain thirdPartyLocal
      f.allPeers(public).peerSpec.localAddressOpt shouldBe None
      f.allPeers(pairedLocal).peerSpec.localAddressOpt shouldBe None
      f.controller ! ConnectTo(f.allPeers(pairedLocal))
      f.tcpManager.expectMsgType[Tcp.Connect].remoteAddress shouldBe pairedLocal
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("an alternate outbound advertisement stays unverified beside the dialed endpoint") {
    val f = new Fixture(maxKnownPeers = 4)
    try {
      f.outgoing(dialed, defaultPeerSpec.copy(declaredAddress = Some(otherClaim)))
      val stored = f.allPeers
      stored(dialed).verifiedOutboundEndpoint shouldBe true
      stored(dialed).peerSpec.declaredAddress shouldBe Some(dialed)
      stored(otherClaim).verifiedOutboundEndpoint shouldBe false

      val query = TestProbe()(f.system)
      query.send(f.peerManager, SeenPeers(10))
      val shared = query.expectMsgType[Seq[PeerInfo]].flatMap(_.peerSpec.declaredAddress)
      shared should contain(dialed)
      shared should not contain otherClaim
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("verified outbound endpoint survives database and actor-system reopen") {
    val f = new Fixture(maxKnownPeers = 3)
    var restartedSystem: Option[ActorSystem] = None
    try {
      f.outgoing(dialed, defaultPeerSpec.copy(declaredAddress = Some(dialed)))
      f.allPeers(dialed).verifiedOutboundEndpoint shouldBe true
      Await.result(f.system.terminate(), Duration.Inf)

      val reopenedDb = new PeerDatabase(f.config, 3)
      try {
        reopenedDb.get(dialed).get.verifiedOutboundEndpoint shouldBe true
      } finally {
        reopenedDb.close()
      }

      val restarted = ActorSystem("PeerEndpointProvenanceRestart")
      restartedSystem = Some(restarted)
      val manager = restarted.actorOf(PeerManagerRef.props(f.config, f.context, 3))
      val query = TestProbe()(restarted)
      query.send(manager, GetAllPeers)
      val reopened = query.expectMsgType[Map[InetSocketAddress, PeerInfo]]
      reopened(dialed).verifiedOutboundEndpoint shouldBe true
      query.send(manager, SeenPeers(10))
      query.expectMsgType[Seq[PeerInfo]].flatMap(_.peerSpec.declaredAddress) should contain(dialed)
    } finally {
      restartedSystem.foreach(s => Await.result(s.terminate(), Duration.Inf))
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("one verified outbound endpoint remains reachable among many legacy rows") {
    val historicalHandshake = System.currentTimeMillis() - 24.hours.toMillis
    val legacyPeers = (1 to 2048).map { i =>
      val address = new InetSocketAddress(s"198.18.${i / 256}.${i % 256}", 9000 + i)
      PeerInfo(defaultPeerSpec.copy(declaredAddress = Some(address)),
        historicalHandshake, Some(Outgoing))
    }
    val f = new Fixture(maxKnownPeers = 2051, persistedPeers = legacyPeers)
    try {
      f.outgoing(dialed, defaultPeerSpec.copy(declaredAddress = Some(dialed)))
      f.allPeers.size shouldBe 2051

      val query = TestProbe()(f.system)
      (1 to 8).foreach { _ =>
        query.send(f.peerManager, SeenPeers(8))
        query.expectMsgType[Seq[PeerInfo]].flatMap(_.peerSpec.declaredAddress) shouldBe Seq(dialed)
      }
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a matching LAN address is a candidate only when local dialing is allowed") {
    val lanAddress = new InetSocketAddress("192.168.44.200", 9030)
    val inboundSource = new InetSocketAddress(lanAddress.getAddress, 55002)
    val advertised = defaultPeerSpec.copy(
      declaredAddress = Some(otherClaim),
      features = Seq(LocalAddressPeerFeature(lanAddress))
    )

    Seq(true, false).foreach { allowLocal =>
      val f = new Fixture(maxKnownPeers = 3, allowLocal = allowLocal)
      try {
        f.incoming(inboundSource, advertised)
        val chosen = if (allowLocal) lanAddress else otherClaim
        val rejected = if (allowLocal) otherClaim else lanAddress
        val stored = f.allPeers
        stored.keySet should contain(chosen)
        stored.keySet should not contain rejected
        stored(chosen).lastHandshake shouldBe 0L
        stored(chosen).connectionType shouldBe None
        stored(chosen).peerSpec.localAddressOpt shouldBe None
      } finally {
        Await.result(f.system.terminate(), Duration.Inf)
        deleteRecursive(f.directory)
      }
    }
  }

  property("an IPv6 unique local claim is not discovered when local dialing is disabled") {
    val f = new Fixture(maxKnownPeers = 3, allowLocal = false)
    try {
      val ulaClaim = new InetSocketAddress("fd12:3456::1", 9036)
      val inboundSource = new InetSocketAddress("198.51.100.41", 55003)
      f.incoming(inboundSource, defaultPeerSpec.copy(declaredAddress = Some(ulaClaim)))
      f.allPeers.keySet should not contain ulaClaim
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("an allowed IPv6 unique local link sends a usable declared address on the handshake wire") {
    val localBind = new InetSocketAddress("fd12:3456::2", settings.scorexSettings.network.bindAddress.getPort)
    val f = new Fixture(maxKnownPeers = 3, allowLocal = true, bindAddress = localBind)
    try {
      val remote = new InetSocketAddress("fd12:3456::1", 55004)
      val ownHandshake = f.incomingWithLocal(remote, localBind, defaultPeerSpec.copy(declaredAddress = Some(remote)))
      ownHandshake.peerSpec.declaredAddress shouldBe Some(localBind)
      ownHandshake.peerSpec.localAddressOpt shouldBe None
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a pending dial does not turn an inbound socket into a verified outbound endpoint") {
    val f = new Fixture
    try {
      f.beginDial(claimed)
      val inbound = f.incoming(claimed, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      inbound.remote.connectionId.direction shouldBe Incoming
      f.allPeers(claimed).lastHandshake shouldBe f.oldHandshake

      val outbound = f.completedDial(claimed, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      outbound.remote.connectionId.direction shouldBe Outgoing
      f.allPeers(claimed).lastHandshake should be > f.oldHandshake
      f.lifecycle.expectMsgType[HandshakedPeer].remote.handlerRef shouldBe inbound.remote.handlerRef
      f.lifecycle.expectMsgType[DisconnectedPeer].peer.handlerRef shouldBe inbound.remote.handlerRef
      f.lifecycle.expectMsgType[HandshakedPeer].remote.handlerRef shouldBe outbound.remote.handlerRef

      val liveMessage = Message[Unit](GetPeersSpec, Right(()), Some(outbound.remote))
      f.controller ! liveMessage
      f.forwardedMessages.expectMsg(liveMessage)
      f.controller ! Message[Unit](GetPeersSpec, Right(()), Some(inbound.remote))
      f.forwardedMessages.expectNoMessage(200.millis)

      f.system.stop(inbound.remote.handlerRef)
      f.lifecycle.expectNoMessage(200.millis)
      f.connectedPeers.map(_.handlerRef) should contain(outbound.remote.handlerRef)
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("closing an inbound socket leaves an unrelated pending dial active") {
    val f = new Fixture
    try {
      f.beginDial(claimed)
      val inbound = f.incoming(claimed, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      inbound.remote.connectionId.direction shouldBe Incoming
      f.system.stop(inbound.remote.handlerRef)
      f.lifecycle.expectMsgType[HandshakedPeer].remote.handlerRef shouldBe inbound.remote.handlerRef
      f.lifecycle.expectMsgType[DisconnectedPeer].peer.handlerRef shouldBe inbound.remote.handlerRef

      val outbound = f.completedDial(claimed, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      outbound.remote.connectionId.direction shouldBe Outgoing
      f.allPeers(claimed).lastHandshake should be > f.oldHandshake
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a dynamic listener port distinguishes an inbound socket from its pending dial") {
    val requestedBind = new InetSocketAddress(settings.scorexSettings.network.bindAddress.getAddress, 0)
    val f = new Fixture(bindAddress = requestedBind)
    try {
      val actualBind = new InetSocketAddress(requestedBind.getAddress, 9120)
      f.tcpManager.send(f.controller, Tcp.Bound(actualBind))
      f.beginDial(claimed)
      val inbound = f.incomingAtLocal(claimed, actualBind, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      inbound.remote.connectionId.direction shouldBe Incoming
      f.allPeers(claimed).lastHandshake shouldBe f.oldHandshake

      val outbound = f.completedDial(claimed, defaultPeerSpec.copy(declaredAddress = Some(claimed)))
      outbound.remote.connectionId.direction shouldBe Outgoing
      f.allPeers(claimed).lastHandshake should be > f.oldHandshake
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("a dynamic IPv4 listener advertises its actual port in the local-address feature") {
    val requestedBind = new InetSocketAddress(settings.scorexSettings.network.bindAddress.getAddress, 0)
    val f = new Fixture(maxKnownPeers = 3, allowLocal = true, bindAddress = requestedBind)
    try {
      val actualBind = new InetSocketAddress(requestedBind.getAddress, 9120)
      val local = new InetSocketAddress("192.168.44.201", actualBind.getPort)
      val remote = new InetSocketAddress("192.168.44.202", 55012)
      f.tcpManager.send(f.controller, Tcp.Bound(actualBind))
      val ownHandshake = f.incomingWithLocal(remote, local, defaultPeerSpec.copy(declaredAddress = Some(remote)))
      ownHandshake.peerSpec.localAddressOpt shouldBe Some(local)
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }

  property("an outbound IPv6 unique local link advertises its bound listening endpoint") {
    val localBind = new InetSocketAddress("fd12:3456::2", settings.scorexSettings.network.bindAddress.getPort)
    val f = new Fixture(maxKnownPeers = 3, allowLocal = true, bindAddress = localBind)
    try {
      val remote = new InetSocketAddress("fd12:3456::3", 9037)
      val localEphemeral = new InetSocketAddress(localBind.getAddress, 55011)
      f.beginDial(remote)
      val (event, ownHandshake) = f.completedDialAtLocal(
        remote, localEphemeral, defaultPeerSpec.copy(declaredAddress = Some(remote)))
      event.remote.connectionId.direction shouldBe Outgoing
      ownHandshake.peerSpec.declaredAddress shouldBe Some(localBind)
      ownHandshake.peerSpec.localAddressOpt shouldBe None
      f.allPeers(remote).connectionType shouldBe Some(Outgoing)
    } finally {
      Await.result(f.system.terminate(), Duration.Inf)
      deleteRecursive(f.directory)
    }
  }
}
