package io.bigpanda.kafka.admin

import akka.stream.scaladsl.{ Flow, Sink }
import io.bigpanda.kafka.Common.{ ClientId, GroupId, TopicName }
import io.bigpanda.kafka._
import io.bigpanda.kafka.implicits._
import org.scalatest.time._

import scala.concurrent.duration._

class GetGroupOffsetSpec extends ActorSpec with KafkaSupport {

  override implicit val patienceConfig = PatienceConfig(Span(30, Seconds))

  val producerSettings = ReferenceConfig.producerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")
  val consumerSettings = ReferenceConfig.consumerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")

  "GetGroupOffset" must {
    "get a group's offset properly" in {
      val topicName = TopicName("get-offset-test-topic")
      val groupId = GroupId("test-group")

      TestUtils
        .writeMessages(producerSettings, 20, topicName)
        .futureValue

      GetGroupOffset
        .getGroupOffset(consumerSettings, topicName, groupId, 30.seconds)
        .futureValue shouldEqual 0

      SetGroupOffset
        .setGroupOffset(consumerSettings, topicName, groupId, 16, 30.seconds)
        .futureValue

      GetGroupOffset
        .getGroupOffset(consumerSettings, topicName, groupId, 30.seconds)
        .futureValue shouldEqual 16
    }

    "not interfere with a plain consumer" in {
      val topicName = TopicName("interference-topic")
      val groupId = GroupId("interference-group")

      TestUtils.writeMessages(producerSettings, 20, topicName).futureValue

      GetGroupOffset
        .getGroupOffset(consumerSettings, topicName, groupId, 30.seconds)
        .futureValue shouldEqual 0L

      Consumer
        .committableConsumer[String](consumerSettings, ClientId("test"), groupId, Set(topicName))
        .commit(10)
        .take(20)
        .runWith(Sink.seq)
        .futureValue should have size 20

      GetGroupOffset
        .getGroupOffset(consumerSettings, topicName, groupId, 30.seconds)
        .futureValue shouldEqual 20L

      TestUtils.writeMessages(producerSettings, 20, topicName).futureValue

      Consumer
        .committableConsumer[String](consumerSettings, ClientId("test"), groupId, Set(topicName))
        .commit(10)
        .take(20)
        .runWith(Sink.seq)
        .futureValue should have size 20

      GetGroupOffset
        .getGroupOffset(consumerSettings, topicName, groupId, 30.seconds)
        .futureValue shouldEqual 40L
    }
  }
}
