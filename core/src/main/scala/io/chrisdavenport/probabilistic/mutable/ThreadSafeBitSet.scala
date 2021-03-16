package io.chrisdavenport.probabilistic
package mutable

import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.Breaks
import scala.util.hashing.MurmurHash3
import scala.collection.BitSet

// More Like a BitVector that a bitset, but the name is what Scala calls this
class ThreadSafeBitSet private (
  private final val numLongsPerSegment: Int,
  private final val log2SegmentSize: Int,
  private final val segmentMask: Int,
  private final val segments: AtomicReference[ThreadSafeBitSet.ThreadSafeBitSegments]
) { // TODO extends scala.collection.mutable.BitSet

  /*
   * --------------------
   * Modifications
   * --------------------
   */
  def set(position: Long): Unit = {
    val segmentPosition = position >>> log2SegmentSize // which segment -- div by num bits per segment
    val longPosition = (position >>> 6) & segmentMask // which long in the segment -- remainder of div by num bits per segment
    val bitPosition = position & 0x3F // which bit in the long -- remainder of div by num bits in long (64) -- positive bits
    val segment = getSegment(segmentPosition.toInt)
    val mask = 1L << bitPosition
    var retry = true
    while (retry){
      val currentLongValue = segment.get(longPosition.toInt)
      val newLongValue = currentLongValue | mask
      if (segment.compareAndSet(longPosition.toInt, currentLongValue, newLongValue)){
        retry = false
      }
    }
  }

  def clear(position: Long): Unit = {
    val segmentPosition = position >>> log2SegmentSize // which segment -- div by num bits per segment
    val longPosition = (position >>> 6) & segmentMask // which long in the segment -- remainder of div by num bits per segment
    val bitPosition = position & 0x3F /// which bit in the long -- remainder of div by num bits in long (64)
    val segment = getSegment(segmentPosition.toInt)
    val mask = ~(1L << bitPosition)
    var retry = true
    while (retry){
      val currentLongValue = segment.get(longPosition.toInt)
      val newLongValue = currentLongValue & mask
      if (segment.compareAndSet(longPosition.toInt, currentLongValue, newLongValue)){
        retry = false
      }
    }
  }
  def get(position: Long): Boolean = {
    val segmentPosition = position >>> log2SegmentSize // which segment -- div by num bits per segment
    val longPosition = (position >>> 6) & segmentMask // which long in the segment -- remainder of div by num bits per segment
    val bitPosition = position & 0x3F /// which bit in the long -- remainder of div by num bits in long (64)
    val segment = getSegment(segmentPosition.toInt)
    val mask = 1L << bitPosition
    (segment.get(longPosition.toInt) & mask) != 0
  }

  /**
    * Clear all bits to 0.
    */
  def clearAll(): Unit = {
    val visibleSegments = segments.get
    for {
      i <- 0 until visibleSegments.numSegments
      segment = visibleSegments.getSegment(i)
      j <- 0 until segment.length()
    } {
      segment.set(j, 0L)
    }
  }

  /*
   * --------------------
   * Informational
   * --------------------
   */

  def maxSetBit: Long = {
    val breaks = new Breaks
    val viewableSegments = segments.get()
    var bitPosition = -1L
    breaks.breakable{
      for {
        segmentIdx <- (viewableSegments.numSegments - 1) to 0 by -1
        segment = viewableSegments.getSegment(segmentIdx)
        longIdx <- (segment.length() - 1) to 0 by -1
      } {
        val l = segment.get(longIdx)
        if (l != 0) {
          bitPosition = (segmentIdx.toLong << log2SegmentSize) + (longIdx * 64) + (63 - java.lang.Long.numberOfLeadingZeros(l))
          breaks.break()
        }
      }
    }
    bitPosition
  }

  def nextSetBit(fromIndex: Long): Long = {
    require(fromIndex >= 0, s"fromIndex must be >= 0: got $fromIndex")
    var segmentPosition = fromIndex >>> log2SegmentSize
    val viewableSegments = segments.get()
    if (segmentPosition >= viewableSegments.numSegments) -1
    else {
      var longPosition = (fromIndex >>> 6) & segmentMask // which long in the segment -- remainder of div by num bits per segment
      val bitPosition = fromIndex & 0x3F // which bit in the long -- remainder of div by num bits in long (64)
      var segment = viewableSegments.getSegment(segmentPosition.toInt)
      var word = segment.get(longPosition.toInt) & (0xffffffffffffffffL << bitPosition)
      var response = -1L
      var loop = true
      while (loop) {
        if (word != 0) {
          response = (segmentPosition << (log2SegmentSize)) + (longPosition << 6) + java.lang.Long.numberOfTrailingZeros(word)
          loop = false
        } else {
          longPosition += 1
          if (longPosition > segmentMask) {
            segmentPosition += 1
            if (segmentPosition >= viewableSegments.numSegments) {
              loop = false
              // No bits set, return -
            } else {
              segment = viewableSegments.getSegment(segmentPosition.toInt)
              longPosition = 0
              word = segment.get(longPosition.toInt)
            }
          } else {
            word = segment.get(longPosition.toInt)
          }
        }
      }

      response
    }
  }

  /**
   * The numbers of bits which are set in this bit set.
   **/
  def cardinality: Long = {
    val viewableSegments = segments.get()
    var numSetBits = 0L
    for {
      i <- 0 until viewableSegments.numSegments
      segment = viewableSegments.getSegment(i)
      j <- 0 until segment.length()
    } {
      numSetBits += java.lang.Long.bitCount(segment.get(j))
    }
    numSetBits
  }

  /**
    * The number of bits which are currently specified by this bit set. This
    * is the maximum number which you might need to iterate if you were to
    * iterate over all the bits in this set.
    */
  def currentCapacity: Int = 
    segments.get().numSegments * (1 << log2SegmentSize)


  def eqv(other: ThreadSafeBitSet): Boolean = {
    require(other.log2SegmentSize == log2SegmentSize, "Segment sizes must be the same")
    val thisSegments = segments.get
    val otherSegments = other.segments.get
    var allEqual = true

    val breaks = new Breaks
    
    breaks.breakable{
      // Check All of That Equal to All of This
      for {
        i <- 0 until thisSegments.numSegments
        thisArray = thisSegments.getSegment(i)
        otherArray = {
          if (i < otherSegments.numSegments) Some(otherSegments.getSegment(i))
          else None
        }
        j <- 0 until thisArray.length()
      } {
          val thisLong = thisArray.get(j)
          val otherLong = otherArray.map(_.get(j)).getOrElse(0L)
          if (thisLong != otherLong) {
            allEqual = false
            breaks.break()
          }
        }
      // Check that anything left in that is equal to 0
      for {
        i <- thisSegments.numSegments until otherSegments.numSegments
        otherArray = otherSegments.getSegment(i)
        j <- 0 until otherArray.length
      } {
        val l = otherArray.get(j)
        if (l != 0) {
          allEqual = false
          breaks.break()
        }
      }
    }

    allEqual
  }

  /**
   * Return a new bit set which contains all bits which are contained in this bit set, and which are NOT contained in the `other` bit set.
   *
   * In other words, return a new bit set, which is a bitwise and with the bitwise not of the other bit set.
   *
   */
  def andNot(other: ThreadSafeBitSet): ThreadSafeBitSet = {
    require(other.log2SegmentSize == log2SegmentSize, "Segment sizes must be the same")
    val thisSegments = segments.get()
    val otherSegments = other.segments.get()
    val newSegments = ThreadSafeBitSet.ThreadSafeBitSegments(thisSegments.numSegments, numLongsPerSegment)
    for {
      i <- 0 until thisSegments.numSegments
      thisArray = thisSegments.getSegment(i)
      otherArray = {
        if (i < otherSegments.numSegments) Some(otherSegments.getSegment(i))
        else None
      }
      newArray = newSegments.getSegment(i)
      j <- 0 until thisArray.length()
    } {
      val thisLong = thisArray.get(j)
      val otherLong = otherArray.fold(0L)(a => a.get(j))
      newArray.set(j, thisLong & ~ otherLong)
    }
    val andNot = ThreadSafeBitSet(log2SegmentSize)
    andNot.segments.set(newSegments)
    andNot
  }

  // Get the segment at `segmentIndex`.  If this segment does not yet exist, create it.
  def getSegment(segmentIndex: Int): AtomicLongArray = {
    var visibleSegments = segments.get

    while(visibleSegments.numSegments <= segmentIndex){
      // Thread safety:  newVisibleSegments contains all of the segments from the currently visible segments, plus extra.
      // all of the segments in the currently visible segments are canonical and will not change.
      val newVisibleSegments = ThreadSafeBitSet.ThreadSafeBitSegments(visibleSegments, segmentIndex + 1, numLongsPerSegment)

      // because we are using a compareAndSet, if this thread "wins the race" and successfully sets this variable, then the segments
      // which are newly defined in newVisibleSegments become canonical.
      if (segments.compareAndSet(visibleSegments, newVisibleSegments)) {
        visibleSegments = newVisibleSegments
      } else {
        // If we "lose the race" and are growing the ThreadSafeBitSet segments larger,
        // then we will gather the new canonical sets from the update which we missed on the next iteration of this loop.
        // Newly defined segments in newVisibleSegments will be discarded, they do not get to become canonical.
        visibleSegments = segments.get();
      }
    }

    visibleSegments.getSegment(segmentIndex)
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: ThreadSafeBitSet => eqv(that)
    case _ => false
  }

  override def hashCode(): Int = {
    31 * log2SegmentSize + 
    MurmurHash3.arrayHash(segments.get().segments)
  }

  // Only works if Int Capable values are there
  def toMutableBitSet: scala.collection.mutable.BitSet = {
    val resultSet = scala.collection.mutable.BitSet.empty
    var ordinal = nextSetBit(0)
    while(ordinal != -1){
      resultSet.add(ordinal.toInt)
      ordinal = nextSetBit(ordinal + 1)
    }
    resultSet
  }

  override def toString(): String = {
    val longs = new scala.collection.mutable.ListBuffer[Long]()
    var ordinal = nextSetBit(0)
    while(ordinal != -1L){
      longs.addOne(ordinal)
      ordinal = nextSetBit(ordinal + 1)
    }
    "ThreadSafeBitSet(" ++ longs.mkString(",") + ")"
  }
}

object ThreadSafeBitSet {
  val DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS = 14

  // TODO: Overloads
  // def apply(): ThreadSafeBitSet = apply(DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS)
  // def apply(log2SegmentSize: Int): ThreadSafeBitSet = apply(log2SegmentSize, 0)
  def apply(
    log2SegmentSizeInBits: Int = DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS,
    numBitsToPreallocate: Long = 0L
  ): ThreadSafeBitSet = {
    require(log2SegmentSizeInBits > 6, "Cannot specify fewer than 64 bits in each segment!")
    val log2SegmentSize = log2SegmentSizeInBits
    val numLongsPerSegment = (1 << (log2SegmentSizeInBits - 6))
    val segmentMask = numLongsPerSegment - 1
    val numBitsPerSegment = numLongsPerSegment * 64
    val numSegmentsToPreallocate =
      if (numBitsToPreallocate == 0) 1
      else ((numBitsToPreallocate - 1) / numBitsPerSegment) + 1
    val segments = new AtomicReference[ThreadSafeBitSegments]()
    segments.set(ThreadSafeBitSegments(numSegmentsToPreallocate.toInt, numLongsPerSegment))

    new ThreadSafeBitSet(numLongsPerSegment, log2SegmentSize, segmentMask, segments)
  }

  def fromBitSet(
    bitSet: BitSet,
    log2SegmentSize: Int = DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS
  ): ThreadSafeBitSet = {
    val tsb = apply(log2SegmentSize, bitSet.size.toLong)
    bitSet.foreach(i => 
      tsb.set(i.toLong)
    )
    tsb
  }

  // def orAll(bitSets: ThreadSafeBitSet*): ThreadSafeBitSet = {
  //   ???
  // }

  private class ThreadSafeBitSegments private (private[ThreadSafeBitSet] final val segments: Array[AtomicLongArray]){
    def numSegments = segments.length
    def getSegment(index: Int) = segments(index)
  }
  private object ThreadSafeBitSegments {
    def apply(numSegments: Int, segmentLength: Int) = {
      val segments = new Array[AtomicLongArray](numSegments)
      for(i <- 0 until numSegments) {
        segments.update(i, new AtomicLongArray(segmentLength))
      }
      new ThreadSafeBitSegments(segments)
    }
    def apply(copyFrom: ThreadSafeBitSegments, numSegments: Int, segmentLength: Int) = {
      val segments = new Array[AtomicLongArray](numSegments)
      for(i <- 0 until numSegments) {
        val set = if (i < copyFrom.numSegments) copyFrom.getSegment(i) else new AtomicLongArray(segmentLength)
        segments.update(i, set)
      }
      new ThreadSafeBitSegments(segments)
    }
  }
}