package io.bigpanda.kafka.admin

import akka.actor.ActorSystem
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{
  PathChildrenCache,
  PathChildrenCacheEvent,
  PathChildrenCacheListener
}
import org.apache.zookeeper.CreateMode

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

object TopicDeletion {
  val TopicsDir = "/brokers/topics"
  val DeletionsDir = "/admin/delete_topics"

  def topicDeletionPath(topic: String) = s"${DeletionsDir}/${topic}"

  sealed trait DeletionResult
  object DeletionResult {
    case object Deleted extends DeletionResult
    case object Nonexistent extends DeletionResult
  }

  case class DeletionTimedOut(topic: String)
      extends Exception(s"Topic deletion timed out for ${topic}")

  /**
    * Ask Kafka to delete the specified topic. If Kafka doesn't respond within the
    * specified timeout, the returned Future will be failed.
    *
    * If the topic does not exist, the result will reflect that.
    *
    * The ExecutionContext passed to this method (and all other methods in this module)
    * should be suitable for running blocking operations; e.g., a fixed-size thread-pool-
    * based ExecutionContext is ideal.
    */
  def deleteTopic(
    curator: CuratorFramework,
    timeout: FiniteDuration,
    topic: String
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[DeletionResult] =
    for {
      exists <- topicExists(curator, topic)
      result <- if (exists) startDeletion(curator, timeout, topic)
               else Future.successful(DeletionResult.Nonexistent)
    } yield result

  /**
    * Check if the specified topic exists.
    */
  def topicExists(
    curator: CuratorFramework,
    topic: String
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean] =
    Future {
      Option(curator.checkExists().forPath(s"${TopicsDir}/${topic}")).isDefined
    }

  /**
    * Create a ZNode in the specified path.
    */
  def createZNode(
    curator: CuratorFramework,
    path: String
  )(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    Future {
      curator.create().withMode(CreateMode.PERSISTENT).forPath(path)
    }

  /**
    * Delete the ZNode at the specified path.
    */
  def deleteZNode(
    curator: CuratorFramework,
    path: String
  )(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    Future {
      curator.delete().guaranteed().forPath(path)
    }

  /**
    * Start the topic deletion. Used by deleteTopic; does not check if the
    * topic exists.
    */
  def startDeletion(
    curator: CuratorFramework,
    timeout: FiniteDuration,
    topic: String
  )(implicit
    ec: ExecutionContext,
    system: ActorSystem
  ): Future[DeletionResult] = {
    val promise = Promise[Unit]()

    val pathWatcher = Future(new PathChildrenCache(curator, DeletionsDir, false))

    val deletionListener = new PathChildrenCacheListener {
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit =
        if (event.getData.getPath == topicDeletionPath(topic) && event.getType == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
          promise.trySuccess(())
        }
    }

    val deletionTimer = system.scheduler.scheduleOnce(timeout) {
      if (!promise.isCompleted)
        promise.tryFailure(DeletionTimedOut(topic))
    }

    def cleanup(): Future[Unit] = {
      deletionTimer.cancel()
      pathWatcher.map(_.getListenable.removeListener(deletionListener))
    }

    for {
      watcher <- pathWatcher
      _ = watcher.getListenable.addListener(deletionListener)
      _ = watcher.start()
      _ <- createZNode(curator, topicDeletionPath(topic))
      _ <- promise.future.recoverWith {
            case e =>
              for {
                _ <- cleanup()
                _ <- deleteZNode(curator, topicDeletionPath(topic))
                _ <- Future.failed(e)
              } yield ()
          }
      _ <- cleanup()
    } yield DeletionResult.Deleted
  }
}
