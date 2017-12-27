package io.bigpanda.kafka
package admin

import Common._
import akka.stream.scaladsl.Sink
import org.scalatest.time._
import scala.concurrent.duration._

class SetGroupOffsetSpec extends ActorSpec with KafkaSupport {
  import implicits._

  override implicit val patienceConfig = PatienceConfig(Span(30, Seconds))

  val producerSettings = ReferenceConfig.producerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")
  val consumerSettings = ReferenceConfig.consumerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")

  "SetGroupOffset" must {
    "change a group's offset properly" in {
      val topicName = TopicName("test-topic")
      val groupId = GroupId("test-group")

      TestUtils.writeMessages(producerSettings, 100, TopicName("test-topic")).futureValue
      SetGroupOffset
        .setGroupOffset(consumerSettings, topicName, groupId, 49, 30.seconds)
        .futureValue

      Consumer
        .committableConsumer[String](
          consumerSettings,
          ClientId("clientId"),
          groupId,
          topicName
        )
        .take(1)
        .runWith(Sink.head)
        .futureValue
        .env
        .record
        .offset shouldBe 49L
    }
  }
}
