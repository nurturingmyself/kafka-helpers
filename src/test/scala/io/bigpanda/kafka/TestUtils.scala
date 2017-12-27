package io.bigpanda.kafka

import Common._
import akka.Done
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.kafka.{ scaladsl => ReactiveKafka }
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json._
import scala.concurrent.Future

object TestUtils {
  def readFinite[T: Reads](
    consumerSettings: ConsumerSettings,
    topic: TopicName,
    startOffset: Long,
    endOffset: Long
  ) =
    Consumer.plainConsumer[T](
      consumerSettings,
      ClientId("client"),
      GroupId("group"),
      topic,
      startOffset,
      Some(endOffset)
    )

  def readCommittable[T: Reads](consumerSettings: ConsumerSettings, topic: TopicName) =
    Consumer.committableConsumer[T](
      consumerSettings,
      ClientId("clientId"),
      GroupId("groupId"),
      topic
    )

  def writeMessages(
    producerSettings: ProducerSettings,
    amount: Int,
    topicName: TopicName
  )(implicit
    mat: ActorMaterializer
  ): Future[Done] =
    Source(List.fill(amount)("150"))
      .map { data =>
        akka.kafka.ProducerMessage.Message(
          new ProducerRecord(
            topicName.name,
            0,
            System.currentTimeMillis(),
            "".getBytes("UTF-8"),
            data.getBytes("UTF-8")
          ),
          ()
        )
      }
      .via(ReactiveKafka.Producer.flow(producerSettings))
      .toMat(Sink.ignore)(Keep.right)
      .run()
}
