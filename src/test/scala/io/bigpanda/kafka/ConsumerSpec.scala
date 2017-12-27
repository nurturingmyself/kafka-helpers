package io.bigpanda.kafka

import Common._
import TestUtils._
import akka.stream.scaladsl._
import akka.kafka.{ scaladsl => ReactiveKafka }
import cats.implicits._
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.time._
import play.api.libs.json.JsResult
import scala.util.Random
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class ConsumerSpec extends ActorSpec with KafkaSupport {
  import implicits._

  override implicit val patienceConfig = PatienceConfig(Span(30, Seconds))

  val producerSettings = ReferenceConfig.producerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")
  val consumerSettings = ReferenceConfig.consumerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")

  "A plainConsumer graph" when {
    "using mapMessage and mapMessageF" must {
      "transform messages" in withTopic { topicName =>
        writeMessages(producerSettings, 100, topicName)

        val res = Consumer
          .plainConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId    = ClientId("client"),
            groupId     = GroupId("group"),
            topic       = topicName,
            startOffset = 0,
            endOffset   = Some(99)
          )
          .mapMessageF(_.asOpt)
          .mapMessage(_.toString)
          .runWith(Sink.seq)
          .futureValue
          .map(_.fa)

        res should have size 100
        res should contain only Some("150")
      }
    }

    "wrapPlainFlow" must {
      "transform messages" in withTopic { topicName =>
        writeMessages(producerSettings, 100, topicName)

        val res = Consumer
          .plainConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId    = ClientId("client"),
            groupId     = GroupId("group"),
            topic       = topicName,
            startOffset = 0,
            endOffset   = Some(99)
          )
          .wrapFlow {
            Flow[JsResult[Int]].map(_.asOpt).map(_.map(_.toString))
          }
          .runWith(Sink.seq)
          .futureValue
          .map(_.fa)

        res should have size 100
        res should contain only Some("150")
      }
    }
  }

  "A committableConsumer graph" when {
    "using mapMessage and mapMessageF" must {
      "transform messages" in withTopic { topicName =>
        writeMessages(producerSettings, 100, topicName)

        val res = Consumer
          .committableConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId = ClientId("client"),
            groupId  = GroupId("group"),
            topics   = Set(topicName)
          )
          .take(100)
          .mapMessageF(_.asOpt)
          .mapMessage(_.toString)
          .runWith(Sink.seq)
          .futureValue
          .map(_.fa)

        res should have size 100
        res should contain only Some("150")
      }

      "commit offsets" in withTopic { topicName =>
        writeMessages(producerSettings, 100, topicName)

        val (control, res) = Consumer
          .committableConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId = ClientId("client"),
            groupId  = GroupId("group"),
            topics   = Set(topicName)
          )
          .commit(100)
          .take(100)
          .toMat(Sink.seq)(Keep.both)
          .run()

        val msgs = res.futureValue.map(_.env)
        msgs.map(_.record.offset()) should contain theSameElementsAs (0 to 99)
        control.shutdown().futureValue

        writeMessages(producerSettings, 50, topicName)

        val (control2, res2) = Consumer
          .committableConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId = ClientId("client2"),
            groupId  = GroupId("group"),
            topics   = Set(topicName)
          )
          .commit(50)
          .take(50)
          .toMat(Sink.seq)(Keep.both)
          .run()

        val msgs2 = res2.futureValue.map(_.env)
        msgs2.map(_.record.offset()) should contain theSameElementsAs (100 to 149)
        control2.shutdown().futureValue
      }
    }

    "using wrapCommittableFlow" must {
      "transform messages" in withTopic { topicName =>
        writeMessages(producerSettings, 100, topicName)

        val res = Consumer
          .committableConsumer[Int](
            settings = ReferenceConfig.consumerSettings.withBootstrapServers(
              s"localhost:${embeddedKafkaConfig.kafkaPort}"),
            clientId = ClientId("client"),
            groupId  = GroupId("group"),
            topics   = Set(topicName)
          )
          .take(100)
          .wrapFlow {
            Flow[JsResult[Int]].map(_.asOpt).map(_.map(_.toString))
          }
          .runWith(Sink.seq)
          .futureValue
          .map(_.fa)

        res should have size 100
        res should contain only Some("150")
      }
    }
  }

  def withTopic(test: TopicName => Unit): Unit = {
    val topicName = s"topic-${Random.nextInt}"
    test(TopicName(topicName))
  }
}
