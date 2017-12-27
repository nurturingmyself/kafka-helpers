package io.bigpanda.kafka.admin

import akka.actor.ActorSystem
import io.bigpanda.kafka.Common._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

object GetGroupOffset {

  /**
    * Get the last committed offset for the given groupId and topic.
    *
    */
  def getGroupOffset(
    settings: ConsumerSettings,
    topic: TopicName,
    groupId: GroupId,
    timeout: FiniteDuration
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[Long] =
    Future {
      val consumer = settings
        .withGroupId(groupId.id)
        .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        .createKafkaConsumer()

      val wakeupTask = system.scheduler.scheduleOnce(timeout) {
        consumer.wakeup()
      }

      try {
        consumer.subscribe(List(topic.name).asJava)
        consumer.poll(settings.pollTimeout.toMillis)

        consumer.assignment().asScala.headOption match {
          case Some(_) =>
            val topicPartition = new TopicPartition(topic.name, 0)

            Option(consumer.committed(topicPartition)).map(_.offset()).getOrElse(0L)
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
