package io.bigpanda.kafka

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.Directory

trait KafkaSupport extends BeforeAndAfterAll { self: UnitSpec =>
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map(
      "delete.topic.enable" -> "true"
    ),
    kafkaPort     = scala.util.Random.nextInt(32768) + 1024,
    zooKeeperPort = scala.util.Random.nextInt(32768) + 1024
  )

  private val kafkaDirectory = Directory.makeTemp("kafka")
  private val zkDirectory = Directory.makeTemp("zookeeper")

  abstract override def beforeAll(): Unit = {
    super.beforeAll()

    EmbeddedKafka.startZooKeeper(zkDirectory)
    EmbeddedKafka.startKafka(kafkaDirectory)
  }

  abstract override def afterAll(): Unit = {
    EmbeddedKafka.stopKafka()
    EmbeddedKafka.stopZooKeeper()

    kafkaDirectory.deleteRecursively()
    zkDirectory.deleteRecursively()

    super.afterAll()
  }
}
