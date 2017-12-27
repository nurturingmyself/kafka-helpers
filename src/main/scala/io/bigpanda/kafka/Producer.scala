package io.bigpanda.kafka

import Common.{ Producer => KafkaProducer, _ }
import akka.NotUsed
import akka.stream.scaladsl._
import cats.implicits._, cats._
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.producer.{ Callback, RecordMetadata }
import play.api.libs.json._

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.language.higherKinds

/**
  * A typeclass representing a conversion from T to a tuple of an optional
  * key, a message body and an optional timestamp.
  */
trait KafkaWritable[T] {
  import KafkaWritable._

  def toWritable(t: T): (Option[Key], Value, Option[Timestamp])
}

object KafkaWritable {
  case class Key(data: Array[Byte]) extends AnyVal
  case class Value(data: Array[Byte]) extends AnyVal
  case class Timestamp(ts: Long) extends AnyVal

  /**
    * Convenience function for creating a KafkaWritable instance using
    * an instance of a JSON serializer and two functions for keys and timestamps.
    */
  def apply[T: Writes](key: T => Option[Key], timestamp: T => Option[Timestamp]): KafkaWritable[T] =
    new KafkaWritable[T] {
      def toWritable(t: T) =
        (key(t), Value(Json.toJson(t).toString.getBytes("UTF-8")), timestamp(t))
    }

  implicit val producerRecordKafkaWritable: KafkaWritable[ProducerRecord] =
    new KafkaWritable[ProducerRecord] {
      def toWritable(p: ProducerRecord) =
        (Option(p.key).map(Key), Value(p.value), Option(p.timestamp).map(Timestamp(_)))
    }
}

object Producer {

  /**
    * Creates a producer record using the specified data, passthrough and topic name.
    */
  def createProducerRecord[T: KafkaWritable, P](data: T, topic: TopicName): ProducerRecord = {
    val (key, value, timestamp) = implicitly[KafkaWritable[T]].toWritable(data)

    new ProducerRecord(
      topic.name,
      0,
      timestamp.map(t => new java.lang.Long(t.ts)).orNull,
      key.map(_.data).orNull,
      value.data
    )
  }

  /**
    * Write a message in a context F to Kafka using the producer instance. The context
    * must be both Applicative and Traversable. This will accomodate the common effects -
    * Option, Either, Try, and List.
    *
    * The purpose of this function is to accomodate usecases of "optional produce" - that
    * is, only write to Kafka if the data is actually present in the context.
    */
  def produceF[F[_]: Traverse, T: KafkaWritable](
    producer: KafkaProducer,
    topicName: TopicName,
    message: F[T]
  )(implicit
    ec: ExecutionContext
  ): Future[F[ProducerResult[T]]] =
    message traverse { t =>
      val promise = Promise[ProducerResult[T]]()
      val record = createProducerRecord(t, topicName)

      producer.send(
        record,
        new Callback {
          @SuppressWarnings(Array("org.wartremover.warts.Null"))
          def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit =
            if (exception != null)
              promise.failure(exception)
            else
              promise.success(EnvT(recordMetadata, t: Id[T]))
        }
      )

      promise.future
    }

  /**
    * Write the message to the specified topic in Kafka using the producer instance.
    */
  def produce[T: KafkaWritable](
    producer: KafkaProducer,
    topic: TopicName,
    message: T
  )(implicit
    ec: ExecutionContext
  ): Future[ProducerResult[T]] =
    produceF[Id, T](producer, topic, message)

  /**
    * Creates a stage that produces messages wrapped in an effectful context F
    * to the specified topic with the specified parallelism.
    */
  def producer[F[_]: Traverse, E, T: KafkaWritable](
    producer: KafkaProducer,
    parallelism: Int,
    topicName: TopicName
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, F, ProducerResult[T]], NotUsed] =
    Flow[EnvT[E, F, T]].mapAsync(parallelism)(_.traverse(produceF(producer, topicName, (_: Id[T]))))

  /**
    * Convenience method for integrating an offset monitor that monitors the last
    * produced offset. The monitor can be wrapped in any effect that has a Functor.
    */
  def offsetMonitor[F[_]: Functor, E, H[_]: Functor, T](
    monitor: H[AtomicLong]
  ): Flow[EnvT[E, F, ProducerResult[T]], EnvT[E, F, ProducerResult[T]], NotUsed] =
    Flow[EnvT[E, F, ProducerResult[T]]].map { elem =>
      elem.map { e =>
        monitor.map { m =>
          m.set(e.env.offset)
        }
      }

      elem
    }
}
