package io.bigpanda.kafka.admin

import akka.actor.ActorSystem
import io.bigpanda.kafka.Common._
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.JavaConverters._

object SetGroupOffset {

  /**
    * Override the committed offset for the given groupId and topic.
    *
    * The offset given to this function will be the *next* offset that a
    * consumer in this group will read.
    */
  def setGroupOffset(
    settings: ConsumerSettings,
    topic: TopicName,
    groupId: GroupId,
    offset: Long,
    timeout: FiniteDuration
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[Unit] =
    Future {
      val consumer = settings
        .withGroupId(groupId.id)
        .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        .createKafkaConsumer()

      val wakeupTask = system.scheduler.scheduleOnce(timeout) {
        consumer.wakeup()
      }

      try {
        consumer.subscribe(List(topic.name).asJava)
        consumer.poll(settings.pollTimeout.toMillis)

        consumer.assignment().asScala.headOption match {
          case Some(assignment) =>
            val topicPartition = new TopicPartition(topic.name, 0)
            val offsetMetadata = new OffsetAndMetadata(offset)

            consumer.seek(new TopicPartition(topic.name, 0), offset)
            consumer.poll(settings.pollTimeout.toMillis)
            consumer.commitSync(Map(topicPartition -> offsetMetadata).asJava)

          case None =>
            throw new IllegalStateException(s"Empty assignment when subscribing to topic ${topic}")
        }
      } finally {
        wakeupTask.cancel()
        consumer.unsubscribe()
        consumer.close()
      }
    }
}
