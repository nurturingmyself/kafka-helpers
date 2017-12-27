package io.bigpanda.kafka

import cats.Id
import scala.language.higherKinds

object Common {

  /**
    * Type alias for ConsumerSettings[Array[Byte], Array[Byte]].
    */
  type ConsumerSettings = akka.kafka.ConsumerSettings[Array[Byte], Array[Byte]]

  /**
    * Type alias for ProducerSettings[Array[Byte], Array[Byte]].
    */
  type ProducerSettings = akka.kafka.ProducerSettings[Array[Byte], Array[Byte]]

  /**
    * Type alias for KafkaProducer[Array[Byte], Array[Byte]].
    */
  type Producer = org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]]

  /**
    * Type alias for a tuple of reactive-kafka's CommittableMessage[Array[Byte], Array[Byte]]
    * and another type, usually the deserialized message body, wrapped in an effectful context
    * such as `Option`, `JsResult`, etc.
    */
  type CommittableMessage[F[_], A] =
    EnvT[akka.kafka.ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]], F, A]

  /**
    * Type alias for Kafka's ConsumerRecord[Array[Byte], Array[Byte]] and another type,
    * usually the deserialized message body, wrapped in an effectful context such as
    * `Option`, `JsResult`, etc.
    */
  type Message[F[_], A] =
    EnvT[org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]], F, A]

  /**
    * Type alias for ProducerRecord[Array[Byte], Array[Byte]].
    */
  type ProducerRecord = org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]

  /**
    * Type alias for a tuple of Kafka's RecordMetadata and another type, usually the message
    * that has been written to Kafka.
    */
  type ProducerResult[A] = EnvT[org.apache.kafka.clients.producer.RecordMetadata, Id, A]

  case class ClientId(id: String) extends AnyVal
  case class GroupId(id: String) extends AnyVal
  case class TopicName(name: String) extends AnyVal
}
