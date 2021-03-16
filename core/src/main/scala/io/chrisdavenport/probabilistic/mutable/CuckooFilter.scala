package io.chrisdavenport.probabilistic.mutable

import scala.util.Random
import cats.Id
import java.nio.charset.Charset
import io.chrisdavenport.probabilistic.hashes.Hashes
import cats.Contravariant

class CuckooFilter[A] private (
  private[mutable] val table: CuckooTable,
  hash: Array[Byte] => Long,
  f: A => Array[Byte], 
  random: Random,
  maxRelocationAttempts: Int, 
) extends io.chrisdavenport.probabilistic.CuckooFilter[cats.Id, A]{
  import CuckooFilter._

  def add(a: A): Boolean = {
    val h = hash(f(a))
    val h1 = hash1(h)
    val h2 = hash2(h)
    val finger = fingerprint(h2)
    val i1 = index(h1)

    putEntry(finger, i1) ||
    putEntry(finger, index2(i1, finger))
  }
  
  def remove(a: A): Boolean = {
    val h = hash(f(a))
    val h1 = hash1(h)
    val h2 = hash2(h)
    val finger = fingerprint(h2)
    val i1 = index(h1)
    val i2 = index2(i1, finger)
    table.swapAnyEntry(i1, CuckooTable.EMPTY_ENTRY, finger) ||
    table.swapAnyEntry(i2, CuckooTable.EMPTY_ENTRY, finger)
  }
  
  def mayContain(a: A): Boolean = {
    val h = hash(f(a))
    val h1 = hash1(h)
    val h2 = hash2(h)
    val finger = fingerprint(h2)
    val i1 = index(h1)
    val i2 = index2(i1, finger)
    table.findEntry(i1, finger).isDefined ||
    table.findEntry(i2, finger).isDefined
  }

  private def putEntry(fingerprint: Int, index: Long): Boolean = {
    return table.swapAnyEntry(index, fingerprint, CuckooTable.EMPTY_ENTRY) ||
      putEntry(fingerprint,index, 0);
  }


  private def putEntry(fingerprint: Int, index: Long, kick: Int): Boolean = {
    if (maxRelocationAttempts == kick) {
      return false;
    } else {

      val entry = random.nextInt(table.numEntriesPerBucket)
      val kicked = table.writeEntry(index, entry, fingerprint)

      if ((CuckooTable.EMPTY_ENTRY == kicked)
          || putEntry(kicked, index2(index, kicked), kick + 1)) {
        return true;
      } else {
        val kickedBack = table.writeEntry(index,entry, kicked)
        assert(kickedBack == fingerprint, "Uh oh - couldn't unroll failed attempts to putEntry()")
        return false;
      }
    }
  }

  private def hash1(hash: Long): Long = {
    hash
  }

  private def hash2(hash: Long): Long = {
    hash >>> 32
  }


  private def index(hash: Long): Long = {
    mod(hash, table.numBuckets).toInt
  }

  private def index2(index: Long, fingerprint: Int): Long = {
    mod(protectedSum(index, parsign(index) * odd(hash(intToArray(fingerprint))), table.numBuckets), table.numBuckets)
  }



  /**
   * Maps parity of i to a sign.
   *
   * @return 1 if i is even parity, -1 if i is odd parity
   */
  private def parsign(i: Long): Long = {
    return ((i & 0x01L) * -2L) + 1L;
  }

  private def odd(i: Long): Long = {
    i | 0x01L
  }

  private def intToArray(data: Int): Array[Byte] = {
    BigInt(data).toByteArray
  }

    /**
   * Returns the sum of index and offset, reduced by a mod-consistent amount if necessary to
   * protect from numeric overflow. This method is intended to support a subsequent mod operation
   * on the return value.
   *
   * @param index Assumed to be >= 0L.
   * @param offset Any value.
   * @param mod Value used to reduce the result,
   * @return sum of index and offset, reduced by a mod-consistent amount if necessary to protect
   *         from numeric overflow.
   */
  private def protectedSum(index: Long, offset: Long, mod: Long): Long =  {
    if (canSum(index, offset)) index + offset else  protectedSum(index - mod, offset, mod);
  }

  private def canSum(a: Long, b: Long): Boolean = {
    (a ^ b) < 0 | (a ^ (a + b)) >= 0
  }

  /**
    * Returns an f-bit portion of the given hash. Iterating by f-bit segments from the least
    * significant side of the hash to the most significant, looks for a non-zero segment. If a
    * non-zero segment isn't found, 1 is returned to distinguish the fingerprint from a
    * non-entry.
    *
    * @param hash 64-bit hash value
    * @param f number of bits to consider from the hash
    * @return first non-zero f-bit value from hash as an int, or 1 if no non-zero value is found
    */
  private[mutable] def fingerprint(hash: Long): Int = {
    val f = table.numBitsPerEntry

    val mask = (0x80000000 >> (f - 1)) >>> (Integer.SIZE - f)
    var bit = 0
    var ret: Long = 0x1.toLong

    while (bit + f <= Integer.SIZE){
      ret = (hash >> bit) & mask
      if (ret != 0) {
        bit = Integer.SIZE
      } else {
        bit += f
      }
    }
    ret.toInt
  }
  
}

object CuckooFilter {

  def string(numberOfItems: Long, falsePositiveRate: Double)(implicit charset: Charset = Charset.defaultCharset()): CuckooFilter[String] = {
    of(numberOfItems, falsePositiveRate, {s: String => s.getBytes(charset)})
  }

  def array(numberOfItems: Long, falsePositiveRate: Double): CuckooFilter[Array[Byte]] = {
    of(numberOfItems, falsePositiveRate, identity)
  }

  def of[A](numberOfItems: Long, falsePositiveRate: Double, f: A => Array[Byte]): CuckooFilter[A] = {
    val numEntriesPerBucket = optimalEntriesPerBucket(falsePositiveRate)
    val numBuckets: Long = optimalNumberOfBuckets(numberOfItems, numEntriesPerBucket)
    val numBitsPerEntry = optimalBitsPerEntry(falsePositiveRate, numEntriesPerBucket)
    val random = new Random()
    val maxRelocationAttempts = 500

    new CuckooFilter[A](
      CuckooTable(numBuckets, numEntriesPerBucket, numBitsPerEntry),
      Hashes.XXHash.hash(_),
      f,
      random,
      maxRelocationAttempts
    )
  }

  
  private def mod(x: Long, m: Long): Long = {
    val result = x % m
    if (result >= 0) result else result + m
  }
  
  val MAX_ENTRIES_PER_BUCKET = 8
  val MIN_ENTRIES_PER_BUCKET = 2

  /**
   * Minimum false positive probability supported, 8.67E-19.
   *
   * CuckooFilter § 5.1 Eq. (6), "f ≥ log2(2b/e) = [log2(1/e) + log2(2b)]"
   * (b) entries per bucket: 8 at e <= 0.00001
   * (f) bits per entry: 64-bits max
   * (e) false positive probability
   *
   * 64 = log2(16/e) = [log2(1/e) + log2(16)]
   * 64 = log2(1/e) + 4
   * 60 = log2(1/e)
   * 2^60 = 1/e
   * e = 1/2^60
   * e = 8.673617379884035E-19
   */
  val MIN_FPP = 1.0D / Math.pow(2, 60)

  /**
   * Maximum false positive probability supported, 0.99.
   */
  val MAX_FPP = 0.99D

  /*
   * Space optimization cheat sheet, per CuckooFilter § 5.1 :
   *
   * Given:
   *   n: expected insertions
   *   e: expected false positive probability (e.g. 0.03D for 3% fpp)
   *
   * Choose:
   *   b: bucket size in entries (2, 4, 8)
   *   a: load factor (proportional to b)
   *
   * Calculate:
   *   f: fingerprint size in bits
   *   m: table size in buckets
   *
   *
   * 1) Choose b =     8   | 4 |   2
   *      when e : 0.00001 < e ≤ 0.002
   *      ref: CuckooFilter § 5.1 ¶ 5, "Optimal bucket size"
   *
   * 2) Choose a =  50% | 84% | 95.5% | 98%
   *      when b =   1  |  2  |  4    |  8
   *      ref: CuckooFilter § 5.1 ¶ 2, "(1) Larger buckets improve table occupancy"
   *
   * 2) Optimal f = ceil( log2(2b/e) )
   *    ref: CuckooFilter § 5.1 Eq. (6), "f ≥ log2(2b/e) = [log2(1/e) + log2(2b)]"
   *
   * 3) Required m = evenCeil( ceiling( ceiling( n/a ) / b ) )
   *       Minimum entries (B) = n/a rounded up
   *       Minimum buckets (m) = B/b rounded up to an even number
   */

  /**
   * Returns the optimal number of entries per bucket, or bucket size, ({@code b}) given the
   * expected false positive probability ({@code e}).
   *
   * CuckooFilter § 5.1 ¶ 5, "Optimal bucket size"
   *
   * @param e the desired false positive probability (must be positive and less than 1.0)
   * @return optimal number of entries per bucket
   */
  def optimalEntriesPerBucket(e: Double) = {
    require(e > 0.0D, "e must be > 0.0");
    if (e <= 0.00001) {
      MAX_ENTRIES_PER_BUCKET
    } else if (e <= 0.002) {
      MAX_ENTRIES_PER_BUCKET / 2
    } else {
      MIN_ENTRIES_PER_BUCKET;
    }
  }

  /**
   * Returns the optimal load factor ({@code a}) given the number of entries per bucket ({@code
   * b}).
   *
   * CuckooFilter § 5.1 ¶ 2, "(1) Larger buckets improve table occupancy"
   *
   * @param b number of entries per bucket
   * @return load factor, positive and less than 1.0
   */
  def optimalLoadFactor(b: Int): Double = {
    require(b == 2 || b == 4 || b == 8, "b must be 2, 4, or 8");
    if (b == 2) {
      0.84D
    } else if (b == 4) {
      0.955D
    } else {
      0.98D
    }
  }

  private val log2 = (x: Double) => Math.log10(x)/ Math.log10(2.0)

  def optimalBitsPerEntry(e: Double, b: Int): Int = {
    require(e >= MIN_FPP, "Cannot create CuckooFilter with FPP[" + e +
        "] < CuckooFilter.MIN_FPP[" + CuckooFilter.MIN_FPP + "]");
    val d = log2(2 * b / e)
    d.round.toInt
  }

  def optimalNumberOfBuckets(n: Long,b: Int): Long =  {
    require(n > 0, "n must be > 0");
    val x = Math.ceil((math.ceil(n / optimalLoadFactor(b)) / b)).toLong
    (x + 1) / 2 * 2
  }

}