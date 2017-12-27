package io.bigpanda.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.BeforeAndAfterAll

abstract class ActorSpec
    extends TestKit(ActorSystem("kafka-helpers")) with UnitSpec with ImplicitSender
    with BeforeAndAfterAll {
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher
}
