package io.bigpanda.kafka.admin

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Random
import scala.collection.JavaConverters._

object TopicLastPosition {
  type Settings = ConsumerSettings[Array[Byte], Array[Byte]]

  /**
    * Retrieve the last offset in the topic, or None if the topic is empty. Note
    * that this method returns the _actual_ last offset, not the next offset after it.
    *
    * The execution context passed to this method should be fit for blocking
    * I/O usage.
    */
  def topicLastPosition(
    settings: Settings,
    topic: String,
    timeout: FiniteDuration
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[Option[Long]] = Future {

    val bootstrapServers = settings.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    val consumer = new KafkaConsumer(
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG          -> Random.alphanumeric.take(5).mkString,
        // This is set to 1 to ensure that the server gets us the data as fast as possible;
        // values larger than 1 cause Kafka to try and accumulate more data before responding
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG    -> "1",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG  -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
      ).asJava,
      new ByteArrayDeserializer(),
      new ByteArrayDeserializer()
    )

    val wakeupTask = system.scheduler.scheduleOnce(timeout) {
      consumer.wakeup()
    }

    try {
      consumer.subscribe(List(topic).asJava)
      consumer.poll(settings.pollTimeout.toMillis)

      consumer.assignment().asScala.headOption match {
        case Some(assignment) =>
          val position = consumer.position(assignment)

          if (position == 0) None
          // Kafka will return the next offset that'll be consumed when querying
          // the position method, so to get the actual latest offset for the topic,
          // we need to subtract one.
          else Some(position - 1)

        case None =>
          throw new IllegalStateException(
            s"Empty assignment when querying topic position for ${topic}")
      }
    } finally {
      wakeupTask.cancel()
      consumer.unsubscribe()
      consumer.close()
    }
  }
}
