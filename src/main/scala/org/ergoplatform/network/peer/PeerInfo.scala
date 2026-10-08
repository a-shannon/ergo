package org.ergoplatform.network.peer

import org.ergoplatform.network.{PeerSpec, PeerSpecSerializer, Version}

import java.net.InetSocketAddress
import scorex.core.network.{ConnectionDirection, Incoming, Outgoing}
import org.ergoplatform.serialization.ErgoSerializer
import scorex.util.serialization.{Reader, Writer}

/**
  * Information about peer to be stored in PeerDatabase
  *
  * In case of updating this class check database backwards compatibility also!
  *
  * @param peerSpec       - general information about the peer
  * @param lastHandshake  - timestamp when last handshake was done
  * @param connectionType - type of connection (Incoming/Outgoing) established to this peer if any
  * @param lastStoredActivityTime - timestamp when peer was last seen active
  * @param verifiedOutboundEndpoint - exact declared endpoint accepted after an outbound handshake
  */
case class PeerInfo(peerSpec: PeerSpec,
                    lastHandshake: Long,
                    connectionType: Option[ConnectionDirection] = None,
                    lastStoredActivityTime: Long = 0L,
                    verifiedOutboundEndpoint: Boolean = false)

/**
  * Information about P2P layer status
  *
  * @param lastIncomingMessage - timestamp of last received message from any peer
  * @param currentNetworkTime  - current network time
  */
case class PeersStatus(lastIncomingMessage: Long, currentNetworkTime: Long)

object PeerInfo {

  /**
    * Create peer info from address only, when we don't know other fields
    * (e.g. we got this information from config or from API)
    */
  def fromAddress(address: InetSocketAddress): PeerInfo = {
    val peerSpec = PeerSpec("unknown", Version.initial, s"unknown-$address", Some(address), Seq())
    PeerInfo(peerSpec, 0L, None, 0L)
  }

}

/**
  * Serializer of [[org.ergoplatform.network.peer.PeerInfo]]
  */
object PeerInfoSerializer extends ErgoSerializer[PeerInfo] {

  override def serialize(obj: PeerInfo, w: Writer): Unit = {
    w.putLong(obj.lastHandshake)
    w.putOption(obj.connectionType)((w,d) => w.putBoolean(d.isIncoming))
    PeerSpecSerializer.serialize(obj.peerSpec, w)
    w.putBoolean(obj.verifiedOutboundEndpoint)
  }

   override def parse(r: Reader): PeerInfo = {
     val lastHandshake = r.getLong()
     val connectionType = r.getOption(if (r.getUByte() != 0) Incoming else Outgoing)
     val peerSpec = PeerSpecSerializer.parse(r)
     val verifiedOutboundEndpoint = r.remaining match {
       case 0 => false // records written before endpoint provenance was persisted
       case 1 => r.getUByte() match {
         case 0 => false
         case 1 => true
         case flag => throw new IllegalArgumentException(s"Invalid endpoint proof flag: $flag")
       }
       case count => throw new IllegalArgumentException(s"Unexpected peer-info trailing bytes: $count")
     }
     PeerInfo(peerSpec, lastHandshake, connectionType,
       lastStoredActivityTime = 0L, verifiedOutboundEndpoint = verifiedOutboundEndpoint)
   }
}
