package io.bigpanda.kafka

import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

trait UnitSpec
    extends WordSpecLike with Matchers with OptionValues with TryValues with ScalaFutures {}
