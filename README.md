# kafka-helpers

[![Download](https://api.bintray.com/packages/bigpanda/oss-maven/kafka-helpers/images/download.svg)](https://bintray.com/bigpanda/oss-maven/kafka-helpers/_latestVersion)
[![Build Status](https://travis-ci.org/bigpandaio/kafka-helpers.svg?branch=master)](https://travis-ci.org/bigpandaio/kafka-helpers)

An assortment of `akka-streams` stages and utility functions for working with Kafka. The library builds on `reactive-kafka` and provides combinators that show up frequently in graphs based on Kafka.

## What's inside?

The functionality is arranged in several modules:
* `ReferenceConfig` - sensible defaults for `reactive-kafka`'s `ConsumerSettings` and `ProducerSettings`;
* `Consumer` - stages for creating a committable/plain Kafka source, transforming messages from that stream and committing offsets
* `Producer` - stages for producing messages to Kafka; also supports optional production of messages wrapped in effects like `Option`, `Either`, etc.
* `Common` - type aliases for the data types used in the above modules
* `admin` - contains a utility for deleting topics and retrieving the last offset on a given topic

All methods are documented with scaladoc.

## Reference configuration

The library provides reference `ProducerSettings` and `ConsumerSettings` under `io.bigpanda.kafka.ReferenceConfig`.

Note that you need to provide both settings with the servers using the `.withBootstrapServers` method.

The `ConsumerSettings` also needs to be provided with the `clientId` and `groupId` using the `withClientId` and `withGroupId` methods, respectively.

## A common usage scenario

Let's examine a typical graph that reads and writes Kafka topics. We'll write out the stage types in parentheses. 

The graph needs to do the following:
- Read messages from a Kafka topic (`CommittableMessage[Array[Byte]]`)
- Deserialize them into a type (`CommittableMessage[Array[Byte]] => (CommittableMessage[Array[Byte]], JsResult[T])`)
  - Note that at this point, we have to keep the original message from Kafka, as we'll need to use it to commit the offset later.
- Do some sort of validation and conversion (`JsResult[T] => Option[U]`)
- Produce to Kafka, if the validation succeeded (`Option[U] => Option[Future[ProducerResult]]`)
- Commit the offsets, even if validation did not succeed (`CommittableMessage[Array[Byte]] => Future[CommitResult]`)
  - This stage should also do batching when committing offsets

In most typical use cases, this is how the computation graph will look; consuming messages, deserializing from JSON, optionally producing, committing offsets - all of these stages have logic that can be shared.

So let's see how this graph would look with the stages provided by this library. First, these are the data types we'll work with:
```scala
case class Incident(id: String, timestamp: Long)
case class UpsertIncident(id: String, timestamp: Long)

implicit val incidentReads: Reads[Incident]
implicit val upsertIncidentWrites: Writes[Incident]
```

And we need a few imports, too:
```scala
import io.bigpanda.kafka.Common._
import io.bigpanda.kafka.implicits._
import cats.implicits._
```

Our example graph will transform `Incident` to `UpsertIncident` with some validation, produce to another topic and commit the offsets. Here's how it looks like:
```scala

implicit val upsertKW: KafkaWritable[UpsertIncident] = KafkaWritable(key = (upsert => None), timestamp = (upsert => None))

def validate(incident: Incident): Option[UpsertIncident] =
  if (incident.timestamp % 2 == 0)
    Some(UpsertIncident(id = incident.id, timestamp = incident.timestamp))
  else None
  
val businessLogic: Flow[Option[Incident], Option[UpsertIncident], NotUsed] = 
  Flow[Option[Incident]].map { maybeIncident =>
    maybeIncident.flatMap(validate)
  }

val entireFlow = Consumer
  .committableConsumer[Incident](
    settings = ReferenceConfig.consumerSettings,
    clientId = ClientId("client"),
    groupId  = GroupId("group"),
    topic    = TopicName("incidents")
  )
  .mapMessageF(_.asOpt)
  .wrapFlow(businessLogic)
  .produce(
    producer    = producer,
    parallelism = 50,
    topic       = TopicName("upserts")
  )
  .commit(100)
  .to(Sink.ignore)
```

We'll go through each stage and see what it does:
* `Consumer.committableConsumer[Incident]` - Creates a message source. Can be invoked for any type `T` that has a `play-json` `Reads` instance. The elements output from this stage are `CommittableMessage[JsResult, Incident]`.

  A `CommittableMessage[F[_], T]` is a fancy way of writing `(akka.kafka.CommittableMessage[Array[Byte], Array[Byte]], F[T])`. It is implemented with a data type called `EnvT` which we will discuss later.

* `mapMessageF` - a stage that lets us transform the `F[T]` value of the `CommittableMessage[F[_], T]`. In this case, we're converting the `JsResult` to an `Option`.

* `wrapFlow` - This stage lets us wrap any plain flow of type `Flow[F[T], F[U], _]` and convert it to a flow of type `Flow[CommittableMessage[F, T], CommittableMessage[F, U], _]`. 

  In the example, the `businessLogic` flow is exactly the logic that cannot be shared. Separating it from the main flow will allow us to cleanly unit test it without any messy mocks.
  
* `produce` - This stage pipes messages into the specified topic in Kafka. It uses an external producer and allows you to share the producer instance across the application, improving throughput. 

  The stage requires an implicit `KafkaWritable` instance for the input type; this is a typeclass that describes how to serialize a message into a triplet of key, value and timestamp. The example 
  shows how to create an instance succinctly for any type that has a `play-json` `Writes` instance by providing functions for creating the key and timestamp values.
  
* `commit` - This stage receives elements of `CommittableMessage` and commits them. It uses batching to improve performance.

Since in most cases, the actual flow we're writing is a simple function that operates on the actual deserialized messages, and doesn't care about the Kafka metadata - `wrapFlow` provides some big boilerplate savings. You can now write a simple `Flow[F[T], F[U], _]` and unit test it without dealing with mocking Kafka, etc.

## Lower-level details

If the flow we're wrapping is not 1-to-1 or we need to handle a different usecase, it's worth understanding how things work in the library. Please note that you need these imports to get the library's methods to show up on `Source` and `Flow`:
```scala
import cats.implicits._
import io.bigpanda.kafka.implicits._
```

### Data types
An important underlying data type for the library is `EnvT[E, F[_], T]`. This is just a fancy tuple of type `(E, F[T])`. It allows us to represent data of type `T` wrapped in an effect of type `F[_]` along with some metadata `E`. For example, `EnvT[Metadata, Option, Incident]` is essentially `(Metadata, Option[Incident])`.

`EnvT` has a bunch of typeclass instances available for it, depending on the properties of `F[_]`. Check out the companion object for more detils.

Now, the consumer stages can output two data types:
* `CommittableMessage[F[_], T]` - equivalent to `EnvT[akka.kafka.CommittableMessage[Array[Byte], Array[Byte]], F, T]`. Makes it easy to keep the Kafka metadata around, but operate only on the right side.
* `Message[F[_], T]` - equivalent to `EnvT[org.apache.kafka.ConsumerRecord[Array[Byte], Array[Byte]], F, T]`. Similar to the previous data type, but not committable.

The producer stages operate on the above data types, and output one data type:
* `ProducerResult[T]` - equivalent to `EnvT[org.apache.RecordMetadata, Id, T]`. RecordMetadata contains the details about what was written to Kafka (offset, etc). `Id` is an identity effect: `Id[T]` is equivalent to `T`.

### Consumer stages
We have two types of consumer stages:
* `committableConsumer[T]` - a consumer which outputs `CommittableMessage[JsResult, T]`
* `plainConsumer[T]` - a consumer which outputs `Message[JsResult, T]` and reads from a specific start-offset up to a specific end-offset (if specified).

The combination of committable/plain above was chosen through the common scenarios. If you think more should be added, feel free to send a PR (or tell @iravid).

### Transforming messages

The library is designed around the common usecase of working on a stream of `CommittableMessage[F[_], T]`, where `F[_]` is some sort of effect which has a `Traverse` instance - e.g., `Option`, `Either`, `List`, etc.

As such, `mapMessage` lets you apply a function to the `T` element nested inside:
```scala
Flow[CommittableMessage[Option, Int]].mapMessage(_.toString)
// Flow[CommittableMessage[Option, Int], CommittableMessage[Option, String], NotUsed]
```

If you want to change the effect as well, `mapMessageF` lets you operate on the entire `F[T]`:
```scala
Flow[CommittableMessage[JsResult, Int]].mapMessageF {
  case JsSuccess(i, _) => Some(i)
  case e: JsError      => 
    log.error(...)
    None
}
// Flow[CommittableMessage[JsResult, Int], CommittableMessage[Option, Int], NotUsed]
```

There are many more useful combinators. For example, `mapMessageAsync` works like `mapMessage`, but you can return a `Future` and it'll get unwrapped transparently; `flatMapMessage` lets you return a value inside an `F[_]` and it'll get flattened, and so on. See the scaladoc for details.

Of course, all of the above combinators also work for streams of `Message[F[_], T]`.

### Producer stages

Generally, using the `.produce` syntax on flows of `CommittableMessage[F[_], A]` or `Message[F[_], A]` will attach a producer to the stream. The `F[_]` must be traversable. This is not provided currently for elements without an `F[_]`, but you can make this work by specifying `Id` explictly as the effect.

The producer stage requires the `A` type to have a `KafkaWritable` instance. This is a typeclass that represents the function `A => (Option[Key], Value, Option[Timestamp])`.

A convenience function `KafkaWritable.apply` is provided when `A` has a `play-json` `Writes` instance.

### Committing offsets

Offset commits are done using the `.commit` syntax or the `Consumer.offsetCommitter` stage. The stage will batch the commits (dynamically, depending on backpressure from the Kafka driver).

### Monitoring offsets

There are two types of offsets that need to be monitored in Kafka-based streams:
* Last message offset that passed through a stage. This is provided through the `offsetMonitor` method:
```scala
val monitor: Option[AtomicLong]

// Using a Source:
val src: Source[CommittableMessage[Try, Int], _]
val monitoredSrc = src.offsetMonitor(monitor)

// Using a Flow:
val flow: Flow[CommittableMessage[Try, Int], CommittableMessage[Try, Int], _]
val monitoredFlow = flow.offsetMonitor(monitor)
```
  This also works for plain message flows.
  
* Last message offset written to another topic. This is provided through the `producedOffsetMonitor` method:
```scala
val monitor: Option[AtomicLong]

// Using a Source:
val src: Source[CommittableMessage[Try, ProducerResult[Int]], _]
val monitoredSrc = src.producedOffsetMonitor(monitor)

// Using a Flow:
val flow: Flow[CommittableMessage[Try, Int], CommittableMessage[Try, ProducerResult[Int]], _]
val monitoredFlow = flow.producedOffsetMonitor(monitor)
```

These methods use the `Consumer.offsetMonitor` and `Producer.offsetMonitor` stages. The provided `AtomicLong` can be wrapped in any effect that has a `Functor` (`Option`, `Try`, etc. or just `Id`).
