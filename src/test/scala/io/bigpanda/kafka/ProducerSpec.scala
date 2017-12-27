package io.bigpanda.kafka

import akka.kafka.{ scaladsl => ReactiveKafka }
import akka.stream.scaladsl._
import cats.implicits._
import Common._
import TestUtils._
import implicits._
import org.scalatest.time._
import play.api.libs.json.Reads
import scala.util.Random

class ProducerSpec extends ActorSpec with KafkaSupport {
  override implicit val patienceConfig = PatienceConfig(Span(30, Seconds))

  val producerSettings = ReferenceConfig.producerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")
  val consumerSettings = ReferenceConfig.consumerSettings.withBootstrapServers(
    s"localhost:${embeddedKafkaConfig.kafkaPort}")

  implicit val kwi: KafkaWritable[Int] = KafkaWritable(_ => None, _ => None)
  implicit val kws: KafkaWritable[String] = KafkaWritable(_ => None, _ => None)

  "produce" must {
    "produce messages properly" in withTopic { topic =>
      Producer
        .produce(producerSettings.createKafkaProducer(), topic, "hello")
        .futureValue

      val res = readFinite[String](consumerSettings, topic, 0, 0)
        .mapMessageF(_.asOpt)
        .runWith(Sink.seq)
        .futureValue

      res.map(_.fa) should contain(Some("hello"))
    }
  }

  "produceF" must {
    "skip empty messages" in withTopic { topic =>
      Producer
        .produceF(producerSettings.createKafkaProducer, topic, None: Option[String])
        .futureValue shouldBe None
    }

    "produce messages" in withTopic { topic =>
      Producer
        .produceF(producerSettings.createKafkaProducer(), topic, Option("hello"))
        .futureValue

      val res = readFinite[String](consumerSettings, topic, 0, 0)
        .mapMessageF(_.asOpt)
        .runWith(Sink.seq)
        .futureValue

      res.map(_.fa) should contain(Some("hello"))
    }
  }

  "committableProducer" must {
    "produce messages properly" in withTopic { sourceTopic =>
      val destTopic = TopicName(s"topic-${Random.nextInt}")
      writeMessages(producerSettings, 100, sourceTopic)

      val copyResult = readCommittable[Int](consumerSettings, sourceTopic)
        .mapMessageF(_.asOpt)
        .produce(producerSettings.createKafkaProducer(), 100, destTopic)
        .take(100)
        .runWith(Sink.seq)
        .futureValue

      copyResult.map(_.fa.value.env.offset) should contain theSameElementsAs (0 to 99)

      val readCopyResult = readFinite[Int](consumerSettings, destTopic, 0, 99)
        .mapMessageF(_.asOpt)
        .runWith(Sink.seq)
        .futureValue

      readCopyResult.map(_.env.offset()) should contain theSameElementsAs (0 to 99)
    }

    "skip empty messages" in withTopic { sourceTopic =>
      val destTopic = TopicName(s"topic-${Random.nextInt}")
      writeMessages(producerSettings, 100, sourceTopic)

      val copyResult = readCommittable[Int](consumerSettings, sourceTopic)
        .map {
          case EnvT(metadata, js) if metadata.record.offset % 2 == 0 =>
            EnvT(metadata, js.asOpt)
          case EnvT(metadata, js) => EnvT(metadata, Option.empty[Int])
        }
        .produce(producerSettings.createKafkaProducer(), 100, destTopic)
        .take(100)
        .runWith(Sink.seq)
        .futureValue

      copyResult.collect {
        case EnvT(msg, Some(_)) => msg.record.offset()
      } should contain theSameElementsAs (0 to 99 by 2)
    }
  }

  "plainProducer" must {
    "produce messages properly" in withTopic { sourceTopic =>
      val destTopic = TopicName(s"topic-${Random.nextInt}")
      writeMessages(producerSettings, 100, sourceTopic)

      val copyResult = readFinite[Int](consumerSettings, sourceTopic, 0, 99)
        .mapMessageF(_.asOpt)
        .produce(producerSettings.createKafkaProducer(), 100, destTopic)
        .runWith(Sink.seq)
        .futureValue

      copyResult.collect {
        case EnvT(_, Some(p)) => p.env.offset
      } should contain theSameElementsAs (0 to 99)

      val readCopyResult = readFinite[Int](consumerSettings, destTopic, 0, 99)
        .mapMessageF(_.asOpt)
        .runWith(Sink.seq)
        .futureValue

      readCopyResult.map { case EnvT(record, _) => record.offset() } should contain
      theSameElementsAs(0 to 99)
    }

    "skip empty messages" in withTopic { sourceTopic =>
      val destTopic = TopicName(s"topic-${Random.nextInt}")
      writeMessages(producerSettings, 100, sourceTopic)

      val copyResult = readFinite[Int](consumerSettings, sourceTopic, 0, 99)
        .map {
          case EnvT(metadata, js) if metadata.offset % 2 == 0 => EnvT(metadata, js.asOpt)
          case EnvT(metadata, js) => EnvT(metadata, Option.empty[Int])
        }
        .produce(producerSettings.createKafkaProducer(), 100, destTopic)
        .runWith(Sink.seq)
        .futureValue

      copyResult.collect {
        case EnvT(msg, Some(_)) => msg.offset()
      } should contain theSameElementsAs (0 to 99 by 2)
    }
  }

  def withTopic(test: TopicName => Unit): Unit = {
    val topicName = s"topic-${Random.nextInt}"
    test(TopicName(topicName))
  }
}
