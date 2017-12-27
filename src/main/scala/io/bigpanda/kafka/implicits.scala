package io.bigpanda.kafka

import akka.stream.scaladsl._
import cats._
import cats.implicits._
import Common.{ Producer => KafkaProducer, _ }
import java.util.concurrent.atomic.AtomicLong

import akka.kafka.ConsumerMessage.CommittableOffsetBatch

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.higherKinds

object implicits {
  implicit class EnvTSourceOps[F[_], E, T, Mat](val src: Source[EnvT[E, F, T], Mat]) {

    /**
      * Transforms the contents of a message wrapped in an effectful context using
      * the specified function.
      */
    def mapMessage[U](f: T => U)(implicit F: Functor[F]): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.map(f))

    /**
      * Like mapMessage, but transforms the contents using an async function.
      */
    def mapMessageAsync[U](
      parallelism: Int
    )(f: T => Future[U]
    )(implicit
      F: Traverse[F],
      ec: ExecutionContext
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.mapAsync(parallelism)(f))

    /**
      * Like mapMessage, but provides access to the message metadata.
      */
    def mapMessageWithMetadata[U](
      f: (E, T) => U
    )(implicit
      F: Functor[F]
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.mapWithEnv(f))

    /**
      * Like mapMessageAsync, but provides access to the message metadata.
      */
    def mapMessageAsyncWithMetadata[U](
      parallelism: Int
    )(f: (E, T) => Future[U]
    )(implicit
      F: Traverse[F],
      ec: ExecutionContext
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.mapAsyncWithEnv(parallelism)(f))

    /**
      * Like mapMessage, but allows you to change the effectful context. Good for unwrapping
      * messages that are inside a JsResult and re-wrapping them with Option/Either/etc.
      */
    def mapMessageF[G[_], U](f: F[T] => G[U]): Source[EnvT[E, G, U], Mat] =
      src.via(Consumer.mapF(f))

    /**
      * Like mapMessageF, but transforms the contents using an async function.
      */
    def mapMessageFAsync[G[_], U](
      parallelism: Int
    )(f: F[T] => Future[G[U]]
    )(implicit
      ec: ExecutionContext
    ): Source[EnvT[E, G, U], Mat] =
      src.via(Consumer.mapFAsync(parallelism)(f))

    /**
      * Like mapMessageF, but provides access to the message metadata.
      */
    def mapMessageFWithMetadata[G[_], U](f: (E, F[T]) => G[U]): Source[EnvT[E, G, U], Mat] =
      src.via(Consumer.mapFWithEnv(f))

    /**
      * Like mapMessageFAsync, but provides access to the message metadata.
      */
    def mapMessageFAsyncWithMetadata[G[_], U](
      parallelism: Int
    )(f: (E, F[T]) => Future[G[U]]
    )(implicit
      ec: ExecutionContext
    ): Source[EnvT[E, G, U], Mat] =
      src.via(Consumer.mapFAsyncWithEnv(parallelism)(f))

    /**
      * Like mapMessage, but transforms the contents using an effectful function.
      */
    def flatMapMessage[U](
      f: T => F[U]
    )(implicit
      M: Monad[F]
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.flatMap(f))

    /**
      * Like flatMapMessage, but provides access to the message metadata.
      */
    def flatMapMessageWithMetadata[U](
      f: (E, T) => F[U]
    )(implicit
      M: Monad[F]
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.flatMapWithEnv(f))

    /**
      * Like flatMapMessage, but transforms the contents using an async function.
      */
    def flatMapMessageAsync[U](
      parallelism: Int
    )(f: T => Future[F[U]]
    )(implicit
      M: Monad[F],
      T: Traverse[F],
      ec: ExecutionContext
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.flatMapAsync(parallelism)(f))

    /**
      * Like flatMapMessageAsync, but provides access to the message metadata.
      */
    def flatMapMessageAsyncWithMetadata[U](
      parallelism: Int
    )(f: (E, T) => Future[F[U]]
    )(implicit
      M: Monad[F],
      T: Traverse[F],
      ec: ExecutionContext
    ): Source[EnvT[E, F, U], Mat] =
      src.via(Consumer.flatMapAsyncWithEnv(parallelism)(f))

    /**
      * Wraps a flow that transforms a type T to a type U into a flow that operates
      * on messages. The inner flow MUST be 1-to-1 (e.g. it cannot generate more elements),
      * or things will go wrong (offsets attached to wrong elements, deadlocks, etc.).
      */
    def wrapFlow[G[_], U](inner: Flow[F[T], G[U], _]): Source[EnvT[E, G, U], Mat] =
      src.via(Consumer.wrapFlow(inner))

    /**
      * Like `wrapFlow`, but provides control over the materialized value.
      */
    def wrapFlowMat[G[_], U, Mat2, Mat3](
      inner: Flow[F[T], G[U], Mat2]
    )(combineMat: (Mat, Mat2) => Mat3
    ): Source[EnvT[E, G, U], Mat3] =
      src.viaMat(Consumer.wrapFlow(inner))(combineMat)

    /**
      * Produce messages using the specified producer and parallelism to the topic. The
      * effect containing the message contents must be traversable.
      */
    def produce(
      producer: KafkaProducer,
      parallelism: Int,
      topic: TopicName
    )(implicit
      FT: Traverse[F],
      FA: Applicative[F],
      T: KafkaWritable[T],
      ec: ExecutionContext
    ): Source[EnvT[E, F, ProducerResult[T]], Mat] =
      src.via(Producer.producer(producer, parallelism, topic))
  }

  implicit class EnvTFlowOps[F[_], G[_], E, T, U, Mat](
    flow: Flow[EnvT[E, F, T], EnvT[E, G, U], Mat]) {

    /**
      * Transforms the contents of a message wrapped in an effectful context using
      * the specified function.
      */
    def mapMessage[V](
      f: U => V
    )(implicit
      G: Functor[G]
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.map(f))

    /**
      * Like mapMessage, but transforms the contents using an async function.
      */
    def mapMessageAsync[V](
      parallelism: Int
    )(f: U => Future[V]
    )(implicit
      F: Traverse[G],
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.mapAsync(parallelism)(f))

    /**
      * Like mapMessage, but provides access to the message metadata.
      */
    def mapMessageWithMetadata[V](
      f: (E, U) => V
    )(implicit
      G: Functor[G]
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.mapWithEnv(f))

    /**
      * Like mapMessageAsync, but provides access to the message metadata.
      */
    def mapMessageAsyncWithMetadata[V](
      parallelism: Int
    )(f: (E, U) => Future[V]
    )(implicit
      F: Traverse[G],
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.mapAsyncWithEnv(parallelism)(f))

    /**
      * Like mapMessage, but allows you to change the effectful context. Good for unwrapping
      * messages that are inside a JsResult and re-wrapping them with Option/Either/etc.
      */
    def mapMessageF[H[_], V](f: G[U] => H[V]): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat] =
      flow.via(Consumer.mapF(f))

    /**
      * Like mapMessageF, but provides access to the message metadata.
      */
    def mapMessageFWithMetadata[H[_], V](
      f: (E, G[U]) => H[V]
    ): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat] =
      flow.via(Consumer.mapFWithEnv(f))

    /**
      * Like mapMessageF, but transforms the contents using an async function.
      */
    def mapMessageFAsync[H[_]: Traverse, V](
      parallelism: Int
    )(f: G[U] => Future[H[V]]
    )(implicit
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat] =
      flow.via(Consumer.mapFAsync(parallelism)(f))

    /**
      * Like mapMessageFAsync, but provides access to the message metadata.
      */
    def mapMessageFAsyncWithMetadata[H[_]: Traverse, V](
      parallelism: Int
    )(f: (E, G[U]) => Future[H[V]]
    )(implicit
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat] =
      flow.via(Consumer.mapFAsyncWithEnv(parallelism)(f))

    /**
      * Like mapMessage, but transforms the contents using an effectful function.
      */
    def flatMapMessage[V](
      f: U => G[V]
    )(implicit
      G: Monad[G]
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.flatMap(f))

    /**
      * Like flatMapMessage, but provides access to the message metadata.
      */
    def flatMapMessageWithMetadata[V](
      f: (E, U) => G[V]
    )(implicit
      G: Monad[G]
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.flatMapWithEnv(f))

    /**
      * Like flatMapMessage, but transforms the contents using an async function.
      */
    def flatMapMessageAsync[V](
      parallelism: Int
    )(f: U => Future[G[V]]
    )(implicit
      G: Monad[G],
      T: Traverse[G],
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.flatMapAsync(parallelism)(f))

    /**
      * Like flatMapMessageAsync, but provides access to the message metadata.
      */
    def flatMapMessageAsyncWithMetadata[V](
      parallelism: Int
    )(f: (E, U) => Future[G[V]]
    )(implicit
      G: Monad[G],
      T: Traverse[G],
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, G, V], Mat] =
      flow.via(Consumer.flatMapAsyncWithEnv(parallelism)(f))

    /**
      * Wraps a flow that transforms a type T to a type U into a flow that operates
      * on messages. The inner flow MUST be 1-to-1 (e.g. it cannot generate more elements),
      * or things will go wrong (offsets attached to wrong elements, deadlocks, etc.).
      */
    def wrapFlow[H[_], V](inner: Flow[G[U], H[V], _]): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat] =
      flow.via(Consumer.wrapFlow(inner))

    /**
      * Like `wrapFlow`, but provides control over the materialized value.
      */
    def wrapFlowMat[H[_], V, Mat2, Mat3](
      inner: Flow[G[U], H[V], Mat2]
    )(combineMat: (Mat, Mat2) => Mat3
    ): Flow[EnvT[E, F, T], EnvT[E, H, V], Mat3] =
      flow.viaMat(Consumer.wrapFlow(inner))(combineMat)

    /**
      * Produce messages using the specified producer and parallelism to the topic. The
      * effect containing the message contents must be traversable.
      */
    def produce(
      producer: KafkaProducer,
      parallelism: Int,
      topic: TopicName
    )(implicit
      GT: Traverse[G],
      GA: Applicative[G],
      U: KafkaWritable[U],
      ec: ExecutionContext
    ): Flow[EnvT[E, F, T], EnvT[E, G, ProducerResult[U]], Mat] =
      flow.via(Producer.producer(producer, parallelism, topic))
  }

  implicit class CommittableSourceOps[F[_], T, Mat](
    val src: Source[CommittableMessage[F, T], Mat]) {

    /**
      * Batches offsets and passes the resulting batch to the specified stage.
      *
      * Useful for performing custom committing logic; e.g. retrying.
      */
    def commitWith(
      batchSize: Long
    )(committer: Flow[(List[CommittableMessage[F, T]], CommittableOffsetBatch),
                      List[CommittableMessage[F, T]],
                      _]
    )(implicit
      ec: ExecutionContext
    ): Source[CommittableMessage[F, T], Mat] =
      src.via(Consumer.offsetCommitterWith(batchSize)(committer))

    /**
      * Commits batches of offsets to Kafka.
      */
    def commit(
      batchSize: Long
    )(implicit
      ec: ExecutionContext
    ): Source[CommittableMessage[F, T], Mat] =
      src.via(Consumer.offsetCommitter(batchSize))

    /**
      * Convenience method for integrating an offset monitor into a stream of
      * messages. Will write the offset of any message passing through to the monitor.
      *
      * The monitor is wrapped in any F[_] that has a Functor.
      */
    def offsetMonitor[H[_]: Functor](
      monitor: H[AtomicLong]
    ): Source[CommittableMessage[F, T], Mat] =
      src.via(Consumer.monitorCommittable(monitor))
  }

  implicit class CommittableFlowOps[F[_], G[_], T, U, Mat](
    flow: Flow[CommittableMessage[F, T], CommittableMessage[G, U], Mat]) {

    /**
      * Batches offsets and passes the resulting batch to the specified stage.
      *
      * Useful for performing custom committing logic; e.g. retrying.
      */
    def commitWith(
      batchSize: Long
    )(committer: Flow[(List[CommittableMessage[G, U]], CommittableOffsetBatch),
                      List[CommittableMessage[G, U]],
                      _]
    )(implicit
      ec: ExecutionContext
    ): Flow[CommittableMessage[F, T], CommittableMessage[G, U], Mat] =
      flow.via(Consumer.offsetCommitterWith(batchSize)(committer))

    /**
      * Commits batches of offsets to Kafka.
      */
    def commit(
      batchSize: Long
    )(implicit
      ec: ExecutionContext
    ): Flow[CommittableMessage[F, T], CommittableMessage[G, U], Mat] =
      flow.via(Consumer.offsetCommitter(batchSize))

    /**
      * Convenience method for integrating an offset monitor into a stream of
      * messages. Will write the offset of any message passing through to the monitor.
      *
      * The monitor is wrapped in any F[_] that has a Functor.
      */
    def offsetMonitor[H[_]: Functor](
      monitor: H[AtomicLong]
    ): Flow[CommittableMessage[F, T], CommittableMessage[G, U], Mat] =
      flow.via(Consumer.monitorCommittable(monitor))
  }

  implicit class PlainSourceOps[F[_], T, Mat](src: Source[Message[F, T], Mat]) {

    /**
      * Convenience method for integrating an offset monitor into a stream of
      * messages. Will write the offset of any message passing through to the monitor.
      *
      * The monitor is wrapped in any F[_] that has a Functor.
      */
    def offsetMonitor[H[_]: Functor](monitor: H[AtomicLong]): Source[Message[F, T], Mat] =
      src.via(Consumer.monitorPlain(monitor))
  }

  implicit class PlainFlowOps[F[_], G[_], T, U, Mat](
    flow: Flow[Message[F, T], Message[G, U], Mat]) {

    /**
      * Convenience method for integrating an offset monitor into a stream of
      * messages. Will write the offset of any message passing through to the monitor.
      *
      * The monitor is wrapped in any F[_] that has a Functor.
      */
    def offsetMonitor[H[_]: Functor](
      monitor: H[AtomicLong]
    ): Flow[Message[F, T], Message[G, U], Mat] =
      flow.via(Consumer.monitorPlain(monitor))
  }

  implicit class EnvTProducerResultSourceOps[F[_], E, T, Mat](
    src: Source[EnvT[E, F, ProducerResult[T]], Mat]) {

    /**
      * Convenience method for integrating an offset monitor that monitors the last
      * produced offset. The monitor can be wrapped in any effect that has a Functor.
      */
    def producedOffsetMonitor[H[_]: Functor](
      monitor: H[AtomicLong]
    )(implicit
      F: Functor[F]
    ): Source[EnvT[E, F, ProducerResult[T]], Mat] =
      src.via(Producer.offsetMonitor(monitor))
  }

  implicit class EnvTProducerResultFlowOps[F[_], G[_], E, T, U, Mat](
    flow: Flow[EnvT[E, F, T], EnvT[E, G, ProducerResult[U]], Mat]) {

    /**
      * Convenience method for integrating an offset monitor that monitors the last
      * produced offset. The monitor can be wrapped in any effect that has a Functor.
      */
    def producedOffsetMonitor[H[_]: Functor](
      monitor: H[AtomicLong]
    )(implicit
      G: Functor[G]
    ): Flow[EnvT[E, F, T], EnvT[E, G, ProducerResult[U]], Mat] =
      flow.via(Producer.offsetMonitor(monitor))
  }
}
