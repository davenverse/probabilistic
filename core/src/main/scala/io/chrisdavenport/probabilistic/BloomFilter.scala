package io.chrisdavenport.probabilistic


import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent._
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import io.chrisdavenport.probabilistic.hashes.Hashes

trait BloomFilter[F[_], A]{
  def add(a: A): F[Unit]
  // False Positives a Reality
  // False Negatives are not a thing
  def mayContain(a: A): F[Boolean]
}

object BloomFilter {

  def string[F[_]: Sync](numberOfItems: Long, falsePositiveRate: Double)(implicit charset: Charset = Charset.defaultCharset()): F[BloomFilter[F, String]] = {
    Sync[F].delay(mutable.BloomFilter.string(numberOfItems, falsePositiveRate)(charset))
      .map(new BloomFilterImpl[F, String](_))
  }

  def array[F[_]: Sync](numberOfItems: Long, falsePositiveRate: Double): F[BloomFilter[F, Array[Byte]]] = {
    Sync[F].delay(mutable.BloomFilter.array(numberOfItems, falsePositiveRate))
      .map(new BloomFilterImpl[F, Array[Byte]](_))
  }

  def static[F[_]: Sync, A](initBitSize: Long, hashFunctions: A => cats.data.NonEmptyList[Long]): F[BloomFilter[F, A]] = 
    for {
      bf <- Sync[F].delay(mutable.BloomFilter.static(initBitSize, hashFunctions))
    } yield new BloomFilterImpl[F, A](bf)


  implicit def instances[F[_]]: Contravariant[({type X[A] = BloomFilter[F, A]})#X] = new Contravariant[({type X[A] = BloomFilter[F, A]})#X]{
    def contramap[A, B](fa: BloomFilter[F,A])(f: B => A): BloomFilter[F,B] = new BloomFilter[F, B] {
      def add(a: B): F[Unit] = fa.add(f(a))
      def mayContain(a: B): F[Boolean] = fa.mayContain(f(a))
    }
  }

  private class BloomFilterImpl[F[_]: Sync, A](
    underlying: mutable.BloomFilter[A]
  ) extends BloomFilter[F, A]{
    def add(a: A): F[Unit] = Sync[F].delay{
      underlying.add(a)
    }
    def mayContain(a: A): F[Boolean] = Sync[F].delay{
      underlying.mayContain(a)
    }
  }
}
