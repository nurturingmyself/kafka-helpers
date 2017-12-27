package io.bigpanda.kafka

import Common._
import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.FlowShape
import cats.{ Monad, Traverse }
import cats.implicits._, cats.{ CoflatMap, Functor }
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import akka.stream.scaladsl._
import akka.kafka.Subscriptions
import akka.kafka.{ scaladsl => ReactiveKafka }
import play.api.libs.json._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.language.higherKinds

object Consumer {

  /**
    * Creates a Kafka consumer for the specified topic. The messages it outputs are committable
    * using the offsetCommitter stage.
    *
    * The topic will be read starting from the last committed offset for the given groupId.
    */
  def committableConsumer[T: Reads](
    settings: ConsumerSettings,
    clientId: ClientId,
    groupId: GroupId,
    topic: TopicName
  ): Source[CommittableMessage[JsResult, T], ReactiveKafka.Consumer.Control] =
    committableConsumer(settings, clientId, groupId, Set(topic))

  /**
    * Creates a Kafka consumer for the specified topics. The messages it outputs are committable
    * using the offsetCommitter stage.
    *
    * The topics will be read starting from the last committed offset in each topic
    * for the given groupId.
    */
  def committableConsumer[T: Reads](
    settings: ConsumerSettings,
    clientId: ClientId,
    groupId: GroupId,
    topics: Set[TopicName]
  ): Source[CommittableMessage[JsResult, T], ReactiveKafka.Consumer.Control] = {
    val settingsWithIds = settings.withClientId(clientId.id).withGroupId(groupId.id)
    val subscription = Subscriptions.topics(topics.map(_.name))

    ReactiveKafka.Consumer
      .committableSource(settingsWithIds, subscription)
      .map(m => EnvT(m, parseMessage(m.record.value)))
  }

  /**
    * Creates a consumer that starts reading a topic from the specified startOffset
    * and stops reading after the specified endOffset if specified.
    *
    * The messages it outputs are _NOT_ committable. If you need a finite, committable
    * stream, you'll need to compose it yourself, as the takeWhile stage has to be placed
    * after the committer stage.
    */
  def plainConsumer[T: Reads](
    settings: ConsumerSettings,
    clientId: ClientId,
    groupId: GroupId,
    topic: TopicName,
    startOffset: Long,
    endOffset: Option[Long]
  ): Source[Message[JsResult, T], ReactiveKafka.Consumer.Control] = {
    val settingsWithIds = settings.withClientId(clientId.id).withGroupId(groupId.id)
    val subscription =
      Subscriptions.assignmentWithOffset(new TopicPartition(topic.name, 0), startOffset)

    ReactiveKafka.Consumer
      .plainSource(settingsWithIds, subscription)
      .via(
        endOffset match {
          case Some(offset) => stopAfterOffset(offset)(_.offset())
          case None         => Flow.apply
        }
      )
      .map(m => EnvT(m, parseMessage(m.value)))
  }

  /**
    * Runs `map` on the `EnvT` in the stream.
    */
  def map[F[_]: Functor, E, T, U](f: T => U): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.map(_.map(f))

  /**
    * Like `map`, but provides access to the `env` value on `EnvT`.
    */
  def mapWithEnv[F[_]: Functor, E, T, U](
    f: (E, T) => U
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.map(_.mapWithEnv(f))

  /**
    * Like `map`, but unwraps the returned future using `Flow#mapAsync`.
    */
  def mapAsync[F[_]: Traverse, E, T, U](
    parallelism: Int
  )(f: T => Future[U]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.mapAsync(parallelism)(_.traverse(f))

  /**
    * Like `mapAsync`, but provides access to the `env` value on `EnvT`.
    */
  def mapAsyncWithEnv[F[_]: Traverse, E, T, U](
    parallelism: Int
  )(f: (E, T) => Future[U]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.mapAsync(parallelism) { ft =>
      ft.traverse(f(ft.env, _))
    }

  /**
    * Transforms the contents and the context using the provided function.
    */
  def mapF[F[_], G[_], E, T, U](f: F[T] => G[U]): Flow[EnvT[E, F, T], EnvT[E, G, U], NotUsed] =
    Flow.apply.map(_.mapF(f))

  /**
    * Like `mapF`, but provides access to the `env` value on `EnvT`.
    */
  def mapFWithEnv[F[_], G[_], E, T, U](
    f: (E, F[T]) => G[U]
  ): Flow[EnvT[E, F, T], EnvT[E, G, U], NotUsed] =
    Flow.apply.map {
      case EnvT(env, fa) => EnvT(env, f(env, fa))
    }

  /**
    * Like `mapF`, but unwraps the returned future using `Flow#mapAsync`.
    */
  def mapFAsync[F[_], G[_], E, T, U](
    parallelism: Int
  )(f: F[T] => Future[G[U]]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, G, U], NotUsed] =
    Flow.apply.mapAsync(parallelism) { ft =>
      f(ft.fa).map(EnvT(ft.env, _))
    }

  /**
    * Like `mapFAsync`, but provides access to the `env` value on `EnvT`.
    */
  def mapFAsyncWithEnv[F[_], G[_], E, T, U](
    parallelism: Int
  )(f: (E, F[T]) => Future[G[U]]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, G, U], NotUsed] =
    Flow.apply.mapAsync(parallelism) { ft =>
      f(ft.env, ft.fa).map(EnvT(ft.env, _))
    }

  /**
    * Runs `flatMap` on the value inside `EnvT`.
    */
  def flatMap[F[_]: Monad, E, T, U](f: T => F[U]): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.map(_.flatMap(f))

  /**
    * Like `flatMap`, but unwraps the resulting future using `Flow#mapAsync`.
    */
  def flatMapAsync[F[_]: Monad: Traverse, E, T, U](
    parallelism: Int
  )(f: T => Future[F[U]]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.mapAsync(parallelism) { ft =>
      ft.fa.flatTraverse(f).map(EnvT(ft.env, _))
    }

  /**
    * Like `flatMap`, but provides access to the `env` value on `EnvT`.
    */
  def flatMapWithEnv[F[_]: Monad, E, T, U](
    f: (E, T) => F[U]
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.map(_.flatMapWithEnv(f))

  /**
    * Like `flatMapAsync`, but provides access to the `env` value on `EnvT`.
    */
  def flatMapAsyncWithEnv[F[_]: Monad: Traverse, E, T, U](
    parallelism: Int
  )(f: (E, T) => Future[F[U]]
  )(implicit
    ec: ExecutionContext
  ): Flow[EnvT[E, F, T], EnvT[E, F, U], NotUsed] =
    Flow.apply.mapAsync(parallelism) { ft =>
      ft.fa.flatTraverse(f(ft.env, _)).map(EnvT(ft.env, _))
    }

  /**
    * Close the stream after seeing the specified offset, using a function for extracting
    * the offset from the elements.
    */
  def stopAfterOffset[T](offset: Long)(offsetExtractor: T => Long) =
    Flow[T].takeWhile(offsetExtractor(_) < offset, inclusive = true)

  /**
    * Batches offsets of messages and passes them through the supplied stage.
    *
    * Useful for committing in batches.
    */
  def offsetCommitterWith[F[_], T](
    batchSize: Long
  )(committer: Flow[(List[CommittableMessage[F, T]], CommittableOffsetBatch),
                    List[CommittableMessage[F, T]],
                    _]
  )(implicit
    ec: ExecutionContext
  ): Flow[CommittableMessage[F, T], CommittableMessage[F, T], NotUsed] =
    Flow[CommittableMessage[F, T]]
      .batch(batchSize, List(_)) { (msgs, msg) =>
        msg :: msgs
      }
      .map { batch =>
        val reversed = batch.reverse
        val offsetBatch = reversed
          .map(_.env.committableOffset)
          .foldLeft(CommittableOffsetBatch.empty)(_ updated _)

        (reversed, offsetBatch)
      }
      .via(committer)
      .mapConcat(identity)

  /**
    * Commits offsets in batches of batchSize.
    */
  def offsetCommitter[F[_], T](
    batchSize: Long
  )(implicit
    ec: ExecutionContext
  ): Flow[CommittableMessage[F, T], CommittableMessage[F, T], NotUsed] =
    offsetCommitterWith(batchSize) {
      Flow.apply.mapAsync(1) {
        case (msgs, batch) =>
          batch.commitScaladsl().map(_ => msgs)
      }
    }

  /**
    * Safely parses a JSON message while handling parsing exceptions.
    */
  def parseMessage[T: Reads](m: Array[Byte]): JsResult[T] =
    for {
      json <- Try(Json.parse(m))
               .map(JsSuccess(_))
               .getOrElse {
                 val msgAsString = new String(m, "UTF-8")
                 JsError(s"Failed to parse JSON in message: ${msgAsString}")
               }
      body <- json.validate[T]
    } yield body

  /**
    * Convenience method for integrating an offset monitor into a stream of
    * messages. Will write the offset of any message passing through to the monitor.
    *
    * The monitor is wrapped in any G[_] that has a Functor.
    */
  def monitorCommittable[F[_], G[_]: Functor, T](
    monitor: G[AtomicLong]
  ): Flow[CommittableMessage[F, T], CommittableMessage[F, T], NotUsed] =
    Flow[CommittableMessage[F, T]].map {
      case m @ EnvT(metadata, _) =>
        monitor.map(_.set(metadata.record.offset))
        m
    }

  /**
    * Convenience method for integrating an offset monitor into a stream of
    * messages. Will write the offset of any message passing through to the monitor.
    *
    * The monitor is wrapped in any G[_] that has a Functor.
    */
  def monitorPlain[F[_], G[_]: Functor, T](
    monitor: G[AtomicLong]
  ): Flow[Message[F, T], Message[F, T], NotUsed] =
    Flow[Message[F, T]].map {
      case m @ EnvT(metadata, _) =>
        monitor.map(_.set(metadata.offset))
        m
    }

  /**
    * Wraps a flow that transforms a type T to a type U into a flow that operates
    * on committable messages. The inner flow MUST be 1-to-1 (e.g. it cannot generate more elements),
    * or things will go wrong (offsets attached to wrong elements, deadlocks, etc.).
    */
  def wrapFlow[F[_], G[_], E, T, U, Mat](
    inner: Flow[F[T], G[U], Mat]
  ): Flow[EnvT[E, F, T], EnvT[E, G, U], Mat] =
    Flow.fromGraph {
      GraphDSL.create(inner) { implicit b => inner =>
        import GraphDSL.Implicits._

        val unzip = b.add(UnzipWith((m: EnvT[E, F, T]) => (m.env, m.fa)))

        val zip =
          b.add(Zip[E, G[U]])

        val toEnvT = b.add(
          Flow[(E, G[U])]
            .map {
              case (env, fu) => EnvT(env, fu)
            })

        // format: off
        unzip.out0          ~> zip.in0
        unzip.out1 ~> inner ~> zip.in1; zip.out ~> toEnvT.in
        // format: on

        FlowShape(unzip.in, toEnvT.out)
      }
    }
}
