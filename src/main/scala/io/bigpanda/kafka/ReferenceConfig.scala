package io.bigpanda.kafka

import Common._
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer }
import scala.concurrent.duration._

object ReferenceConfig {
  val consumerSettings: ConsumerSettings =
    new akka.kafka.ConsumerSettings(
      properties = Map(
        "receive.buffer.bytes"      -> s"${64 * 1024}",
        "max.partition.fetch.bytes" -> s"${10 * 1024 * 1024}",
        "fetch.min.bytes"           -> "1",
        "enable.auto.commit"        -> "false",
        "auto.offset.reset"         -> "earliest"
      ),
      keyDeserializerOpt   = Some(new ByteArrayDeserializer),
      valueDeserializerOpt = Some(new ByteArrayDeserializer),
      pollInterval         = 250.millis,
      pollTimeout          = 50.millis,
      stopTimeout          = 30.seconds,
      closeTimeout         = 20.seconds,
      commitTimeout        = 15.seconds,
      wakeupTimeout        = 30.seconds,
      maxWakeups           = 10,
      dispatcher           = "akka.kafka.default-dispatcher"
    )

  val producerSettings: ProducerSettings =
    new akka.kafka.ProducerSettings(
      properties = Map(
        "batch.size"       -> s"${16 * 1024}",
        "retries"          -> "5",
        "retry.backoff.ms" -> "5000",
        "max.request.size" -> s"${10 * 1024 * 1024}",
        "acks"             -> "1",
        "compression.type" -> "snappy"
      ),
      keySerializerOpt   = Some(new ByteArraySerializer),
      valueSerializerOpt = Some(new ByteArraySerializer),
      closeTimeout       = 20.seconds,
      parallelism        = 1000,
      dispatcher         = "akka.kafka.default-dispatcher"
    )
}
