package org.ergoplatform.nodeView.mempool

import org.ergoplatform.nodeView.mempool.OrderedTxPool.WeightedTxId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scorex.util.bytesToId

class HistogramStatsSpec extends AnyFlatSpec with Matchers {

  "Fee histogram" should "place future-created transactions in the first bin" in {
    val now = 10000L
    val futureTx = WeightedTxId(bytesToId(Array.fill(32)(1.toByte)), 1L, 7L, now + 1000L)
    val oldTx = WeightedTxId(bytesToId(Array.fill(32)(2.toByte)), 1L, 11L, now - 1000L)

    HistogramStats.getFeeHistogram(now, 2, 1000L, Seq(futureTx, oldTx)).toSeq shouldBe Seq(
      FeeHistogramBin(1, 7L),
      FeeHistogramBin(0, 0L),
      FeeHistogramBin(1, 11L)
    )
  }
}
