package io.bigpanda.kafka.admin

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.bigpanda.kafka.UnitSpec
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.time.{ Seconds, Span }

import scala.concurrent.duration._

class TopicDeletionSpec extends TestKit(ActorSystem("io-stores-tests")) with UnitSpec {
  "The TopicDeletion" must {
    "delete topics properly" in withFixtures { curator =>
      curator.create().forPath(s"${TopicDeletion.TopicsDir}/test_topic")

      import system.dispatcher
      val deletion = TopicDeletion.deleteTopic(curator, 5.seconds, "test_topic")

      awaitAssert {
        Option(curator.checkExists().forPath(s"${TopicDeletion.DeletionsDir}/test_topic")) should not be empty
      }

      curator.delete().guaranteed().forPath(s"${TopicDeletion.DeletionsDir}/test_topic")

      deletion.futureValue shouldBe TopicDeletion.DeletionResult.Deleted
    }

    "respond with Nonexistent when the topic does not exist" in withFixtures { curator =>
      import system.dispatcher
      TopicDeletion
        .deleteTopic(curator, 5.seconds, "test_topic")
        .futureValue shouldBe TopicDeletion.DeletionResult.Nonexistent
    }

    "clean up and report error when deletion times out" in withFixtures { curator =>
      curator.create().forPath(s"${TopicDeletion.TopicsDir}/test_topic")

      import system.dispatcher
      val deletion = TopicDeletion.deleteTopic(curator, 1.seconds, "test_topic")

      awaitAssert {
        Option(curator.checkExists().forPath(s"${TopicDeletion.DeletionsDir}/test_topic")) should not be empty
      }

      implicit val patienceConfig = PatienceConfig(Span(30, Seconds))
      deletion.failed.futureValue shouldBe TopicDeletion.DeletionTimedOut("test_topic")

      awaitAssert {
        Option(curator.checkExists().forPath(s"${TopicDeletion.DeletionsDir}/test_topic")) shouldBe empty
      }
    }
  }

  def withFixtures(test: CuratorFramework => Unit): Unit = {
    val testZookeeper = new TestingServer(true)
    val curator = CuratorFrameworkFactory
      .builder()
      .connectString(testZookeeper.getConnectString)
      .retryPolicy(new RetryForever(100))
      .build()

    curator.start()
    curator.createContainers(TopicDeletion.DeletionsDir)
    curator.createContainers(TopicDeletion.TopicsDir)

    try test(curator)
    finally {
      curator.close()
      testZookeeper.stop()
    }
  }
}
