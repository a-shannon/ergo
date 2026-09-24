package org.ergoplatform.settings.llm_generated

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.ergoplatform.settings.{NodeConfigurationReaders, NodeConfigurationSettings}
import org.ergoplatform.utils.ErgoCorePropertyTest

class MiningCandidateSettingsSpec extends ErgoCorePropertyTest with NodeConfigurationReaders {
  property("clamp configured candidate retention to 1 through 16") {
    Seq(Int.MinValue -> 1, 0 -> 1, 1 -> 1, 3 -> 3, 16 -> 16,
      17 -> 16, Int.MaxValue -> 16).foreach { case (requested, expected) =>
      val config = ConfigFactory.parseString(s"ergo.node.miningCandidateCacheSize = $requested")
        .withFallback(ConfigFactory.load()).resolve()
      config.as[NodeConfigurationSettings]("ergo.node").miningCandidateCacheSize shouldBe expected
    }
  }

  property("retain three candidates when the optional setting is absent") {
    val config = ConfigFactory.load().withoutPath("ergo.node.miningCandidateCacheSize")
    config.as[NodeConfigurationSettings]("ergo.node").miningCandidateCacheSize shouldBe 3
  }
}
