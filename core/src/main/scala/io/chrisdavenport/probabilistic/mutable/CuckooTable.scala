package io.chrisdavenport.probabilistic.mutable

import scala.util.control.Breaks

class CuckooTable private (
  private[mutable] val data: ThreadSafeBitSet,
  val numBuckets: Long,
  val numEntriesPerBucket: Int,
  val numBitsPerEntry: Int
){
  import CuckooTable._

  // 0 indexed
  private def bitOffSet(bucket: Long, entry: Int): Long = {
    ((bucket * numEntriesPerBucket) + entry) * numBitsPerEntry
  }

  def readEntry(bucket: Long, entry: Int): Int = {
    require(bucket <= numBuckets)
    require(entry <= numEntriesPerBucket)
    val offset = bitOffSet(bucket, entry)
    val positions = for {
      x <- (offset until offset + numBitsPerEntry).toList
      if (data.get(x))
    } yield x - offset
    fromBitPositions(positions.map(_.toInt))
  }

  def findEntry(bucket: Long, value: Int): Option[Int] = {
    val break = new Breaks
    var entry = Option.empty[Int]
    break.breakable{
      for {
        i <- 0 until numEntriesPerBucket
      } {
        val x = readEntry(bucket, i)
        if (x == value) {
          entry = Some(i)
          break.break()
        }
      }
    }
    entry
  }

  // 0 indexed
  def writeEntry(bucket: Long, entry: Int, value: Int): Int = {
    require(bucket <= numBuckets)
    require(entry <= numEntriesPerBucket, "Entry Higher Than Allowed")
    // Expensive... But unsafe otherwise
    require(highestBitPosition(value) <= numBitsPerEntry, "Bits of this value are too large")

    val x = readEntry(bucket, entry) // TODO Race Condition - Make atomic or keysemaphore on the combination
    val offset = bitOffSet(bucket, entry)
    val newEntrySet = bitPositions(value).map(offset + _).toSet
    val oldEntrySet = bitPositions(x).map(offset + _).toSet
    val entryBits = (offset until (offset + numBitsPerEntry)).toList
    for {
      x <- entryBits
    } {
      if (newEntrySet.contains(x)) {
        data.set(x)
      }
      else if (oldEntrySet.contains(x)){
        data.clear(x)
      }
    }
    x
  }

  def swapAnyEntry(bucket: Long, valueIn: Int, valueOut: Int): Boolean = {
    findEntry(bucket, valueOut)
      .map(writeEntry(bucket, _, valueIn))
      .map{i => 
        val x = i == valueOut
        assert(x, s"Value Out Was Incorrect got $i expected $valueOut")
        true
      }.getOrElse(false)
  }
}

object CuckooTable {

  val EMPTY_ENTRY: Int = 0x00

  private[mutable] def bitPositions(int: Int): List[Int] = {
    val buffer = new scala.collection.mutable.ListBuffer[Int]()
    var number = int
    var position = 0
    while (number != 0){
      if ((number & 1) != 0) {
        buffer.addOne(position)
      }
      position += 1
      number = number >>> 1
    }
    buffer.toList
  }

  private[mutable] def highestBitPosition(int: Int): Int = {
    bitPositions(int).headOption.getOrElse(0)
  }

  private[mutable] def fromBitPositions(l: List[Int]): Int = {
    var x = 0x00
    l.foreach{bitPosition => 
      val mask = 1 << bitPosition
      x = x | mask
    }
    x
  }

  // Int serves a fingerprint

  def apply(
      numBuckets: Long, // X
      numEntriesPerBucket: Int,
      numBitsPerEntry: Int
    ): CuckooTable = new CuckooTable(
      ThreadSafeBitSet(numBitsToPreallocate = numBuckets * numEntriesPerBucket * numBitsPerEntry), 
      numBuckets, numEntriesPerBucket, numBitsPerEntry
    )
}