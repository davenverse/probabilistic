package io.chrisdavenport.probabilistic

import cats.Contravariant
import cats.syntax.all._
import cats.effect._
import java.nio.charset.Charset

trait CuckooFilter[F[_], A]{
  def add(a: A): F[Boolean]
  def remove(a: A): F[Boolean]

  def mayContain(a: A): F[Boolean]
}

object CuckooFilter {

  def string[F[_]: Sync](numberOfItems: Long, falsePositiveRate: Double)(implicit charset: Charset = Charset.defaultCharset()): F[CuckooFilter[F, String]] = {
    Sync[F].delay(mutable.CuckooFilter.string(numberOfItems, falsePositiveRate)(charset))
      .map(new CuckooFilterImpl[F, String](_))
  }

  def array[F[_]: Sync](numberOfItems: Long, falsePositiveRate: Double): F[CuckooFilter[F, Array[Byte]]] = {
    Sync[F].delay(mutable.CuckooFilter.array(numberOfItems, falsePositiveRate))
      .map(new CuckooFilterImpl[F, Array[Byte]](_))
  }


  implicit def instances[F[_]]: Contravariant[({type X[A] = CuckooFilter[F, A]})#X] = new Contravariant[({type X[A] = CuckooFilter[F, A]})#X]{
    def contramap[A, B](fa: CuckooFilter[F,A])(f: B => A): CuckooFilter[F,B] = new CuckooFilter[F, B] {
      def add(a: B): F[Boolean] = fa.add(f(a))
      def remove(a: B): F[Boolean] = fa.remove(f(a))
      def mayContain(a: B): F[Boolean] = fa.mayContain(f(a))
    }
  }

  private class CuckooFilterImpl[F[_]: Sync, A](underlying: mutable.CuckooFilter[A]) extends CuckooFilter[F, A]{
    def add(a: A): F[Boolean] = Sync[F].delay(underlying.add(a))
    def remove(a: A): F[Boolean] = Sync[F].delay(underlying.remove(a))
    def mayContain(a: A): F[Boolean] = Sync[F].delay(underlying.mayContain(a))
  }
}