package io.bigpanda.kafka

import akka.NotUsed
import akka.stream.scaladsl._
import Common._, implicits._, cats.implicits._
import java.util.concurrent.atomic.AtomicLong
import play.api.libs.json._
import scala.concurrent.duration._
import scala.util.Try

class UsageSpec extends ActorSpec with KafkaSupport {
  case class Incident(id: String, timestamp: Long)
  case class UpsertIncident(id: String, timestamp: Long)

  implicit val incidentReads: Reads[Incident] = Reads { json =>
    for {
      id        <- (json \ "id").validate[String]
      timestamp <- (json \ "timestamp").validate[Long]
    } yield Incident(id, timestamp)
  }

  implicit val upsertIncidentWrites: Writes[UpsertIncident] = Writes { upsert =>
    Json.obj("id" -> upsert.id, "timestamp" -> upsert.timestamp)
  }

  implicit val upsertKW: KafkaWritable[UpsertIncident] = KafkaWritable(_ => None, _ => None)

  def validate(incident: Incident): Option[UpsertIncident] =
    if (incident.timestamp % 2 == 0)
      Some(UpsertIncident(id = incident.id, timestamp = incident.timestamp))
    else None

  "The standard usage" must {
    "compile" when {
      "using wrapFlow" in {
        val producer = ReferenceConfig.producerSettings
          .withBootstrapServers(s"localhost:${embeddedKafkaConfig.kafkaPort}")
          .createKafkaProducer()

        Consumer
          .committableConsumer[Incident](
            settings = ReferenceConfig.consumerSettings,
            clientId = ClientId("client"),
            groupId  = GroupId("group"),
            topic    = TopicName("incidents")
          )
          .mapMessageF(_.asOpt)
          .wrapFlow {
            Flow[Option[Incident]].map(_.flatMap(validate))
          }
          .produce(
            producer    = producer,
            parallelism = 50,
            topic       = TopicName("upserts")
          )
          .commit(100)
          .to(Sink.ignore)

        producer.close()
      }
    }

    "working with offsetMonitors" in {
      val monitor: Option[AtomicLong] = None

      val committableFlow: Flow[CommittableMessage[Try, ProducerResult[Int]],
                                CommittableMessage[Try, ProducerResult[Int]],
                                NotUsed] =
        Flow.apply
      val monitoredFlow: Flow[CommittableMessage[Try, ProducerResult[Int]],
                              CommittableMessage[Try, ProducerResult[Int]],
                              NotUsed] =
        committableFlow.offsetMonitor(monitor)
      val producerMonitoredFlow: Flow[CommittableMessage[Try, ProducerResult[Int]],
                                      CommittableMessage[Try, ProducerResult[Int]],
                                      NotUsed] =
        committableFlow.producedOffsetMonitor(monitor)

      val plainFlow
        : Flow[Message[Try, ProducerResult[Int]], Message[Try, ProducerResult[Int]], NotUsed] =
        Flow.apply
      val monitoredPlainFlow
        : Flow[Message[Try, ProducerResult[Int]], Message[Try, ProducerResult[Int]], NotUsed] =
        plainFlow.offsetMonitor(monitor)
      val producedMonitoredPlainFlow
        : Flow[Message[Try, ProducerResult[Int]], Message[Try, ProducerResult[Int]], NotUsed] =
        plainFlow.producedOffsetMonitor(monitor)

      val committableSrc: Source[CommittableMessage[Try, Int], NotUsed] = Source.empty
      val monitoredCommittableSrc: Source[CommittableMessage[Try, Int], NotUsed] =
        committableSrc.offsetMonitor(monitor)

      val committableProducerResultSrc
        : Source[CommittableMessage[Try, ProducerResult[Int]], NotUsed] = Source.empty
      val producedMonitoredCommittableSrc
        : Source[CommittableMessage[Try, ProducerResult[Int]], NotUsed] =
        committableProducerResultSrc.producedOffsetMonitor(monitor)

      val plainSrc: Source[Message[Try, Int], NotUsed] = Source.empty
      val monitoredPlainSrc: Source[Message[Try, Int], NotUsed] = plainSrc.offsetMonitor(monitor)

      val plainProducerResultSrc: Source[Message[Try, ProducerResult[Int]], NotUsed] = Source.empty
      val producedMonitoredPlainProducerResultSrc
        : Source[Message[Try, ProducerResult[Int]], NotUsed] =
        plainProducerResultSrc.producedOffsetMonitor(monitor)
    }
  }
}
