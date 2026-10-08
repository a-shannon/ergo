package org.ergoplatform.network.peer

import org.ergoplatform.db.DBSpec
import org.ergoplatform.network.{PeerSpec, PeerSpecSerializer}
import org.ergoplatform.settings.ErgoSettings
import org.ergoplatform.utils.ErgoCorePropertyTest
import org.ergoplatform.utils.ErgoNodeTestConstants._
import scorex.db.LDBFactory
import scorex.core.network.Outgoing
import scorex.util.serialization.VLQByteStringWriter

import java.io.File
import java.net.{InetSocketAddress, URL}

class PeerDatabaseSpec extends ErgoCorePropertyTest with DBSpec {

  private def testSettings(dir: File): ErgoSettings =
    settings.copy(directory = dir.getAbsolutePath)

  private def peerInfo(address: InetSocketAddress, lastHandshake: Long): PeerInfo = {
    PeerInfo(
      defaultPeerSpec.copy(declaredAddress = Some(address)),
      lastHandshake,
      None,
      0L
    )
  }

  private def peerInfo(spec: PeerSpec, lastHandshake: Long): PeerInfo = {
    PeerInfo(spec, lastHandshake, None, 0L)
  }

  private def oversizedPeerInfo(address: InetSocketAddress, lastHandshake: Long): PeerInfo = {
    val url = RestApiUrlPeerFeature(new URL("http://example.com/" + ("x" * 220)))
    peerInfo(
      defaultPeerSpec.copy(declaredAddress = Some(address), features = Seq.fill(80)(url)),
      lastHandshake
    )
  }

  private def withDb[T](maxKnownPeers: Int = PeerDatabase.MaxKnownPeers)
                       (body: PeerDatabase => T): T = {
    val dir = createTempDir
    val db = new PeerDatabase(testSettings(dir), maxKnownPeers)
    try {
      body(db)
    } finally {
      db.close()
      deleteRecursive(dir)
    }
  }

  property("PeerDatabase should store and retrieve a known peer") {
    val address = new InetSocketAddress("8.8.8.8", 9001)
    val info = peerInfo(address, System.currentTimeMillis())
    withDb() { db =>
      db.addOrUpdateKnownPeer(info)
      db.get(address) shouldBe Some(info)
      db.knownPeers should contain(address -> info)
    }
  }

  property("PeerDatabase should ignore a peer without a usable address") {
    val info = peerInfo(defaultPeerSpec, System.currentTimeMillis())
    withDb() { db =>
      db.addOrUpdateKnownPeer(info)
      db.knownPeers shouldBe empty
    }
  }

  property("PeerDatabase should cap and evict oldest non-connected peer") {
    val addresses = (1 to 4).map(i => new InetSocketAddress(s"8.8.8.$i", 9000 + i))
    withDb(maxKnownPeers = 3) { db =>
      addresses.zip(Seq(1L, 2L, 3L, 4L)).foreach { case (addr, ts) =>
        db.addOrUpdateKnownPeer(peerInfo(addr, ts))
      }
      db.knownPeers.keys should contain(addresses(1))
      db.knownPeers.keys should contain(addresses(2))
      db.knownPeers.keys should contain(addresses(3))
      db.knownPeers.keys should not contain addresses(0)
    }
  }

  property("PeerDatabase should not evict a connected peer when making room") {
    val addresses = (1 to 4).map(i => new InetSocketAddress(s"8.8.8.$i", 9000 + i))
    val connected = Set(addresses.head)
    withDb(maxKnownPeers = 3) { db =>
      addresses.zip(Seq(1L, 2L, 3L, 4L)).foreach { case (addr, ts) =>
        db.addOrUpdateKnownPeer(peerInfo(addr, ts), connected)
      }
      db.knownPeers.keys should contain(addresses(0))
      db.knownPeers.keys should contain(addresses(2))
      db.knownPeers.keys should contain(addresses(3))
      db.knownPeers.keys should not contain addresses(1)
    }
  }

  property("PeerDatabase should ignore peer older than oldest when full") {
    val addresses = (1 to 3).map(i => new InetSocketAddress(s"8.8.8.$i", 9000 + i))
    val older = new InetSocketAddress("8.8.8.100", 9999)
    withDb(maxKnownPeers = 3) { db =>
      addresses.zip(Seq(10L, 20L, 30L)).foreach { case (addr, ts) =>
        db.addOrUpdateKnownPeer(peerInfo(addr, ts))
      }
      db.addOrUpdateKnownPeer(peerInfo(older, 5L))
      db.knownPeers.keys should not contain older
    }
  }

  property("PeerDatabase should remove only old disconnected peers during cleanup") {
    var connected = Set.empty[InetSocketAddress]
    val oldConnected = new InetSocketAddress("8.8.8.1", 9001)
    val oldDisconnected = new InetSocketAddress("8.8.8.2", 9002)
    val recent = new InetSocketAddress("8.8.8.3", 9003)
    val now = System.currentTimeMillis()
    withDb(maxKnownPeers = 100) { db =>
      connected += oldConnected
      val oldTs = now - PeerDatabase.KnownPeerMaxAgeMs - 1000
      db.addOrUpdateKnownPeer(peerInfo(oldConnected, oldTs), connected)
      db.addOrUpdateKnownPeer(peerInfo(oldDisconnected, oldTs), connected)
      db.addOrUpdateKnownPeer(peerInfo(recent, now - 1000), connected)
      db.removeOldPeers(connected)
      db.knownPeers.keys should contain(oldConnected)
      db.knownPeers.keys should contain(recent)
      db.knownPeers.keys should not contain oldDisconnected
    }
  }

  property("PeerDatabase should persist peers across close and reopen") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val address = new InetSocketAddress("8.8.8.8", 9001)
    val info = peerInfo(address, 123456789L)
    try {
      val db1 = new PeerDatabase(dbSettings)
      db1.addOrUpdateKnownPeer(info)
      db1.close()
      val db2 = new PeerDatabase(dbSettings)
      db2.get(address) shouldBe Some(info)
      db2.knownPeers should contain(address -> info)
      db2.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("PeerInfoSerializer should read pre-proof rows as unverified and reject invalid trailers") {
    val address = new InetSocketAddress("8.8.8.9", 9009)
    val info = peerInfo(address, 123456789L).copy(connectionType = Some(Outgoing))
    // Build the old record independently: handshake, direction, then PeerSpec, no proof byte.
    val oldWriter = new VLQByteStringWriter
    oldWriter.putLong(info.lastHandshake)
    oldWriter.putOption(info.connectionType)((writer, direction) =>
      writer.putBoolean(direction.isIncoming))
    PeerSpecSerializer.serialize(info.peerSpec, oldWriter)
    val oldBytes = oldWriter.toBytes

    val parsed = PeerInfoSerializer.parseBytesTry(oldBytes).get
    parsed shouldBe info
    parsed.verifiedOutboundEndpoint shouldBe false
    val rewritten = PeerInfoSerializer.toBytes(parsed)
    rewritten.length shouldBe oldBytes.length + 1
    rewritten.dropRight(1).sameElements(oldBytes) shouldBe true
    rewritten.last shouldBe 0.toByte

    PeerInfoSerializer.parseBytesTry(oldBytes :+ 1.toByte).get
      .verifiedOutboundEndpoint shouldBe true
    PeerInfoSerializer.parseBytesTry(oldBytes :+ 2.toByte).isFailure shouldBe true
    PeerInfoSerializer.parseBytesTry(oldBytes ++ Array[Byte](0, 0)).isFailure shouldBe true
  }

  property("generic peer update clears verified proof even with a local redirect") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val address = new InetSocketAddress("8.8.8.10", 9010)
    val redirect = new InetSocketAddress("192.168.1.10", 9010)
    val verified = peerInfo(address, 123456789L)
      .copy(connectionType = Some(Outgoing), verifiedOutboundEndpoint = true)
    try {
      val db1 = new PeerDatabase(dbSettings)
      db1.addOrUpdateVerifiedOutboundPeer(verified, address, Set.empty)
      db1.get(address).get.verifiedOutboundEndpoint shouldBe true
      db1.verifiedPeers.keySet shouldBe Set(address)

      val wrongKey = verified.copy(peerSpec = verified.peerSpec.copy(
        declaredAddress = Some(redirect)))
      db1.addOrUpdateVerifiedOutboundPeer(wrongKey, address, Set.empty)
      db1.get(redirect) shouldBe None
      val untrustedRedirect = verified.copy(peerSpec = verified.peerSpec.copy(
        features = Seq(LocalAddressPeerFeature(redirect))))
      db1.addOrUpdateVerifiedOutboundPeer(untrustedRedirect, address, Set.empty)
      db1.get(address).get.peerSpec.localAddressOpt shouldBe None

      db1.addOrUpdateKnownPeer(untrustedRedirect)
      db1.get(address).get.verifiedOutboundEndpoint shouldBe false
      db1.get(address).get.peerSpec.localAddressOpt shouldBe Some(redirect)
      db1.verifiedPeers shouldBe empty

      db1.addOrUpdateVerifiedOutboundPeer(verified, address, Set.empty)
      db1.verifiedPeers.keySet shouldBe Set(address)
      db1.remove(address)
      db1.verifiedPeers shouldBe empty
      db1.addOrUpdateKnownPeer(untrustedRedirect)
      db1.close()

      val db2 = new PeerDatabase(dbSettings)
      db2.get(address).get.verifiedOutboundEndpoint shouldBe false
      db2.get(address).get.peerSpec.localAddressOpt shouldBe Some(redirect)
      db2.verifiedPeers shouldBe empty
      db2.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("PeerDatabase should not reload removed peers") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val address1 = new InetSocketAddress("8.8.8.1", 9001)
    val address2 = new InetSocketAddress("8.8.8.2", 9002)
    try {
      val db1 = new PeerDatabase(dbSettings)
      db1.addOrUpdateKnownPeer(peerInfo(address1, 100L))
      db1.addOrUpdateKnownPeer(peerInfo(address2, 200L))
      db1.remove(address1)
      db1.close()
      val db2 = new PeerDatabase(dbSettings)
      db2.knownPeers.keys should not contain address1
      db2.knownPeers should contain(address2 -> peerInfo(address2, 200L))
      db2.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("PeerDatabase should load only newest peers when persisted set exceeds cap") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val addresses = (1 to 5).map(i => new InetSocketAddress(s"8.8.8.$i", 9000 + i))
    // timestamps permuted independently of insertion/address ordering: the newest
    // timestamps belong to addresses(2), addresses(4) and addresses(0)
    val timestamps = Map(
      addresses(0) -> 40L,
      addresses(1) -> 10L,
      addresses(2) -> 50L,
      addresses(3) -> 20L,
      addresses(4) -> 30L
    )
    try {
      val db1 = new PeerDatabase(dbSettings, maxKnownPeers = 5)
      addresses.foreach { addr =>
        db1.addOrUpdateKnownPeer(peerInfo(addr, timestamps(addr)))
      }
      db1.knownPeers should have size 5
      db1.close()

      // cap shrunk to 3: retention must be driven by timestamps, not store order
      val db2 = new PeerDatabase(dbSettings, maxKnownPeers = 3)
      db2.knownPeers should have size 3
      db2.knownPeers.keys should contain(addresses(0))
      db2.knownPeers.keys should contain(addresses(2))
      db2.knownPeers.keys should contain(addresses(4))
      db2.knownPeers.keys should not contain addresses(1)
      db2.knownPeers.keys should not contain addresses(3)
      db2.close()

      // dropped records must be physically deleted from the store: reopening with
      // the original cap must not resurrect them
      val db3 = new PeerDatabase(dbSettings, maxKnownPeers = 5)
      db3.knownPeers should have size 3
      db3.knownPeers.keys should contain(addresses(0))
      db3.knownPeers.keys should contain(addresses(2))
      db3.knownPeers.keys should contain(addresses(4))
      db3.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("PeerDatabase should physically remove malformed and oversized records on startup") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val address = new InetSocketAddress("8.8.8.8", 9001)
    val garbageKey = Array[Byte](1, 2, 3)
    val garbageValue = Array[Byte](4, 5, 6)
    val oversizedKey = new Array[Byte](PeerDatabase.MaxSerializedPeerAddressSize + 1)
    val oversizedValue = new Array[Byte](PeerDatabase.MaxSerializedPeerInfoSize + 1)
    try {
      val db1 = new PeerDatabase(dbSettings)
      db1.addOrUpdateKnownPeer(peerInfo(address, 100L))
      db1.close()

      // plant malformed records directly into the store
      val rawStore = LDBFactory.createKvDb(s"${dir.getAbsolutePath}/peers")
      rawStore.insert(garbageKey, garbageValue)
      rawStore.insert(oversizedKey, oversizedValue)
      rawStore.close()

      val db2 = new PeerDatabase(dbSettings)
      // valid peer survives, malformed records are skipped
      db2.knownPeers.keys should contain(address)
      db2.knownPeers should have size 1
      db2.close()

      // malformed records are physically removed, not reparsed on every startup
      val db3 = new PeerDatabase(dbSettings)
      db3.knownPeers should have size 1
      db3.close()

      val checkStore = LDBFactory.createKvDb(s"${dir.getAbsolutePath}/peers")
      checkStore.get(garbageKey) shouldBe empty
      checkStore.get(oversizedKey) shouldBe empty
      checkStore.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("PeerDatabase should reject an oversized peer value before evicting a stored peer") {
    val dir = createTempDir
    val dbSettings = testSettings(dir)
    val stored = new InetSocketAddress("8.8.8.1", 9001)
    val oversizedAddress = new InetSocketAddress("8.8.8.2", 9002)
    val oversized = oversizedPeerInfo(oversizedAddress, Long.MaxValue)
    PeerInfoSerializer.toBytes(oversized).length should be > PeerDatabase.MaxSerializedPeerInfoSize
    try {
      val db1 = new PeerDatabase(dbSettings, maxKnownPeers = 1)
      db1.addOrUpdateKnownPeer(peerInfo(stored, 1L))
      db1.addOrUpdateKnownPeer(oversized)
      db1.knownPeers.keys should contain(stored)
      db1.knownPeers.keys should not contain oversizedAddress
      db1.close()

      val db2 = new PeerDatabase(dbSettings, maxKnownPeers = 1)
      db2.knownPeers.keys should contain(stored)
      db2.knownPeers.keys should not contain oversizedAddress
      db2.close()
    } finally {
      deleteRecursive(dir)
    }
  }

  property("a proof byte above the exact peer-row cap is rejected before storage") {
    val address = new InetSocketAddress("8.8.8.11", 9011)
    val baseUrl = "http://a.b/"
    val maxPadding = 255 - baseUrl.length
    def feature(padding: Int): RestApiUrlPeerFeature =
      RestApiUrlPeerFeature(new URL(baseUrl + ("x" * padding)))
    def sizedPeer(longFeatures: Int, firstPadding: Int, secondPadding: Int): PeerInfo =
      peerInfo(defaultPeerSpec.copy(
        declaredAddress = Some(address),
        features = Seq.fill(longFeatures)(feature(maxPadding)) ++
          Seq(feature(firstPadding), feature(secondPadding))
      ), 1L)

    val cap = PeerDatabase.MaxSerializedPeerInfoSize
    val longFeatures = (0 to 125).find { count =>
      val gap = cap + 1 - PeerInfoSerializer.toBytes(sizedPeer(count, 0, 0)).length
      gap >= 0 && gap <= 2 * maxPadding
    }.get
    val gap = cap + 1 - PeerInfoSerializer.toBytes(sizedPeer(longFeatures, 0, 0)).length
    val overCap = (math.max(0, gap - 4) to gap).iterator.flatMap { totalPadding =>
      (0 to maxPadding).iterator.flatMap { firstPadding =>
        val secondPadding = totalPadding - firstPadding
        if (secondPadding >= 0 && secondPadding <= maxPadding) {
          Some(sizedPeer(longFeatures, firstPadding, secondPadding))
        } else None
      }
    }.find(info => PeerInfoSerializer.toBytes(info).length == cap + 1).get
    val bytes = PeerInfoSerializer.toBytes(overCap)
    bytes.length shouldBe cap + 1
    val oldBytes = bytes.dropRight(1)
    oldBytes.length shouldBe cap
    PeerInfoSerializer.parseBytesTry(oldBytes).get.verifiedOutboundEndpoint shouldBe false

    withDb() { db =>
      db.addOrUpdateKnownPeer(overCap)
      db.get(address) shouldBe None
    }
  }

  property("PeerDatabase should retain an existing peer when an oversized update arrives") {
    val address = new InetSocketAddress("8.8.8.3", 9003)
    val original = peerInfo(address, 1L)
    withDb() { db =>
      db.addOrUpdateKnownPeer(original)
      db.addOrUpdateKnownPeer(oversizedPeerInfo(address, 2L))
      db.get(address) shouldBe Some(original)
    }
  }

  property("PeerDatabase should reject an oversized serialized address before storing it") {
    val oversizedAddress = InetSocketAddress.createUnresolved("x" * 1100, 9003)
    withDb() { db =>
      db.addOrUpdateKnownPeer(peerInfo(oversizedAddress, 1L))
      db.knownPeers.keys should not contain oversizedAddress
    }
  }

  property("PeerDatabase should keep untried peers (zero lastHandshake) during cleanup") {
    val untried = new InetSocketAddress("8.8.8.1", 9001)
    val unavailableSeed = new InetSocketAddress("8.8.8.2", 9002)
    val oldHandshaked = new InetSocketAddress("8.8.8.3", 9003)
    val recent = new InetSocketAddress("8.8.8.4", 9004)
    val now = System.currentTimeMillis()
    withDb(maxKnownPeers = 100) { db =>
      db.addOrUpdateKnownPeer(peerInfo(untried, 0L))
      db.addOrUpdateKnownPeer(peerInfo(unavailableSeed, 0L))
      db.addOrUpdateKnownPeer(peerInfo(oldHandshaked, now - PeerDatabase.KnownPeerMaxAgeMs - 1000))
      db.addOrUpdateKnownPeer(peerInfo(recent, now - 1000))
      db.removeOldPeers()
      db.knownPeers.keys should contain(untried)
      db.knownPeers.keys should contain(unavailableSeed)
      db.knownPeers.keys should contain(recent)
      db.knownPeers.keys should not contain oldHandshaked
    }
  }

}
