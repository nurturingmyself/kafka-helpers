package io.bigpanda.kafka

import cats._

import scala.language.higherKinds

/**
  * The Environment comonad transformer. This data type is a fancy way of writing tuple
  * of type `(E, F[A])`. It represents a value of type `A` in an effect `F[_]`, along with
  * some metadata of type `E`.
  *
  * EnvT has some interesting properties, depending on the properties of `F[_]`. For example,
  * it has a Functor if `F` has a Functor; it has a Traverse instance of `F` has a Traverse instance,
  * and so forth.
  *
  * It's quite general, but we use it in this library to represent data coming from Kafka:
  * - `A` is the deserialized representation, e.g. an `Incident`
  * - `F[_]` is the effect of potential failure - `Try`, `JsResult`, `Option`, etc.
  * - `E` is the message metadata; for example, Kafka's `ConsumerRecord[Array[Byte], Array[Byte]]`
  */
case class EnvT[E, F[_], A](env: E, fa: F[A]) {
  def mapF[G[_], B](f: F[A] => G[B]): EnvT[E, G, B] = copy(fa = f(fa))

  def mapWithEnv[B](f: (E, A) => B)(implicit F: Functor[F]): EnvT[E, F, B] =
    copy(fa = F.map(fa)(f(env, _)))

  def flatMap[B](f: A => F[B])(implicit F: Monad[F]): EnvT[E, F, B] =
    copy(fa = F.flatMap(fa)(f))

  def flatMapWithEnv[B](f: (E, A) => F[B])(implicit F: Monad[F]): EnvT[E, F, B] =
    copy(fa = F.flatMap(fa)(f(env, _)))
}

object EnvT extends EnvT0 {
  implicit def functor[E, F[_]](
    implicit
    F: Functor[F]
  ): Functor[EnvT[E, F, ?]] =
    new Functor[EnvT[E, F, ?]] {
      override def map[A, B](fa: EnvT[E, F, A])(f: A => B) =
        EnvT(fa.env, F.map(fa.fa)(f))
    }
}

sealed abstract class EnvT0 extends EnvT1 {
  implicit def traverse[E, F[_]](implicit F: Traverse[F]): Traverse[EnvT[E, F, ?]] =
    new Traverse[EnvT[E, F, ?]] {
      def traverse[G[_], A, B](
        fa: EnvT[E, F, A]
      )(f: A => G[B]
      )(implicit
        G: Applicative[G]
      ): G[EnvT[E, F, B]] =
        G.map(F.traverse(fa.fa)(f))(EnvT(fa.env, _))

      def foldLeft[A, B](fa: EnvT[E, F, A], b: B)(f: (B, A) => B): B =
        F.foldLeft(fa.fa, b)(f)

      def foldRight[A, B](fa: EnvT[E, F, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        F.foldRight(fa.fa, lb)(f)
    }
}

sealed abstract class EnvT1 {
  implicit def comonad[E, W[_]](implicit W: Comonad[W]): Comonad[EnvT[E, W, ?]] =
    new Comonad[EnvT[E, W, ?]] {
      def map[A, B](fa: EnvT[E, W, A])(f: A => B): EnvT[E, W, B] =
        EnvT(fa.env, W.map(fa.fa)(f))

      def coflatMap[A, B](fa: EnvT[E, W, A])(f: EnvT[E, W, A] => B): EnvT[E, W, B] =
        EnvT(fa.env, W.coflatMap(fa.fa)(f.compose(EnvT(fa.env, _))))

      def extract[A](fa: EnvT[E, W, A]): A = W.extract(fa.fa)
    }
}
