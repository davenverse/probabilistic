package io.chrisdavenport.probabilistic.mutable

import io.chrisdavenport.probabilistic.hashes.Hashes
import java.nio.charset.Charset

class BloomFilter[A] private (
  private[mutable] val bitSet: ThreadSafeBitSet,
  initSize: Long,
  hashFunctions: A => cats.data.NonEmptyList[Long]
) extends io.chrisdavenport.probabilistic.BloomFilter[cats.Id, A]{
  private def hashToPosition(l: Long): Long = {
    val modulus = (l % initSize).toInt
    if (modulus >= 0) modulus
    else initSize + modulus
  }

  private def positions(a: A): cats.data.NonEmptyList[Long] = {
    hashFunctions(a).map(hashToPosition)
  }
  
  def add(a: A): Unit = {
    positions(a).toList.foreach{i => 
      bitSet.set(i)
    }
  }

  def mayContain(a: A): Boolean = positions(a).forall(bitSet.get(_))
}

object BloomFilter {
  def static[A](initBitSize: Long, hashFunctions: A => cats.data.NonEmptyList[Long]): BloomFilter[A] = {
    val bits = ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, initBitSize)
    new BloomFilter[A](bits, initBitSize, hashFunctions)
  }

  def string(numberOfItems: Long, falsePositiveRate: Double)(implicit charset: Charset = Charset.defaultCharset()): BloomFilter[String] = {
    val bits = optimalNumberOfBits(numberOfItems, falsePositiveRate)
    val hashes = optimalNumberOfHashes(numberOfItems, bits)
    static[String](
      bits, 
      {
        (s: String) => 
        val array = s.getBytes(charset)
        cats.data.NonEmptyList(
          s.hashCode(),
          Hashes.arrayHashes.toList.take(hashes - 1).map(f => f(array))
        )
      }
    )
  }

  def array(numberOfItems: Long, falsePositiveRate: Double): BloomFilter[Array[Byte]] = {
    val bits = optimalNumberOfBits(numberOfItems, falsePositiveRate)
    val hashes = optimalNumberOfHashes(numberOfItems, bits)
    static[Array[Byte]](
      bits, 
      {
        (data: Array[Byte]) => 
        cats.data.NonEmptyList(
          Hashes.arrayHashes.head(data),
          Hashes.arrayHashes.tail.take(hashes - 1).map(f => f(data))
        )
      }
    )
  }

  def optimalNumberOfBits(numberOfItems: Long, falsePositiveRate: Double): Long = {
    val p = if (falsePositiveRate == 0) Double.MinValue else falsePositiveRate
    math.ceil(-1 * numberOfItems * math.log(p) / math.log(2) / math.log(2)).toLong
  }

  def optimalNumberOfHashes(numberOfItems: Long, numberOfBits: Long): Int = {
    math.ceil(numberOfBits / numberOfItems * math.log(2)).toInt
  }

}