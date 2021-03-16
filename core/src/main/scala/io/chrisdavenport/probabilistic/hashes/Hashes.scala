package io.chrisdavenport.probabilistic.hashes

import scala.util.hashing.MurmurHash3
import cats.data.NonEmptyList
import java.nio.charset.Charset

object Hashes {

  // TODO MORE HASHING!!! 
  // Need 14 - 17 unique algorithms for extremely small probabilities of false positives
  val arrayHashes: NonEmptyList[Array[Byte] => Long] = NonEmptyList.of(
    XXHash.hash(_),
    FNV32.hash(_),
    Adler32.hash(_),
    Bernstein.hash(_),
    KernighanRitchie.hash(_),
    Murmur3.hash(_),
    CRC16.hash(_),
    CRC32.hash(_),
  )

  object Adler32 {
    def  hash(data: Array[Byte]): Long = {
      val x = new java.util.zip.Adler32()
      x.update(data)
      x.getValue()
    }

  }

  object FNV32 {
    private val FNV1_32_INIT = 0x811c9dc5
    private val FNV1_PRIME_32 = 16777619

    def hash(data: Array[Byte]): Int = {
      var mHash = FNV1_32_INIT
      for { b <- data}{
        mHash ^= (b & 0xff)
        mHash *= FNV1_PRIME_32
      }
      mHash
    }
  }

    /**
    * XMODEM CRC 16 CRC16 - 16-bit Cyclic Redundancy Check (CRC16)
    *
    * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
    * Width                      : 16 bit
    * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
    * Initialization             : 0000
    * Reflect Input byte         : False
    * Reflect Output CRC         : False
    * Xor constant to output CRC : 0000
    * Output for "123456789"     : 31C3
    */
  object CRC16 {
    
    def hash(data: Array[Byte]): Int = {
      var crc: Int = 0
      data.foreach{ b =>
        crc = (crc << 8) ^ table(((crc >>> 8) ^ (b & 0xff)) & 0xff)
      }
      crc & 0xFFFF
    }

    private[CRC16] lazy val table : Array[Int] = Array(
      0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
      0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
      0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
      0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
      0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
      0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
      0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
      0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
      0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
      0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
      0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
      0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
      0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
      0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
      0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
      0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
      0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
      0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
      0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
      0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
      0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
      0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
      0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
      0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
      0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
      0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
      0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
      0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
      0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
      0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
      0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
      0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
    )
  }

  object CRC32 {
    def hash(data: Array[Byte]): Long = {
      val c = new java.util.zip.CRC32()
      c.update(data)
      c.getValue()
    }
  }


  object Bernstein {
    private val INITIAL = 5381
    private val M = 33
    def hash(data: Array[Byte]): Int = {
      var hash = INITIAL
      for {
        x <- data
      } { hash = M * hash + x }
      hash
    }
  }

  object KernighanRitchie {
    private val INITIAL = 0
    private val M = 31
    def hash(data: Array[Byte]): Int = {
      var hash = INITIAL
      for {
        x <- data
      } { hash = M * hash + x }
      hash
    }
  }

  object Murmur3 {
    def hash(data: Array[Byte]): Int = {
      MurmurHash3.bytesHash(data)
    }
  }

  object XXHash {
    private final val PRIME64_1 = 0x9E3779B185EBCA87L
    private final val PRIME64_2 = 0xC2B2AE3D27D4EB4FL
    private final val PRIME64_3 = 0x165667B19E3779F9L
    private final val PRIME64_4 = 0x85EBCA77C2b2AE63L
    private final val PRIME64_5 = 0x27D4EB2F165667C5L
    private final val DEFAULT_SEED = 0L

    def hash(data: Array[Byte]): Long = hash64(data)

    def hash64(data: Array[Byte], seed: Long = DEFAULT_SEED): Long = {
      val length = data.length
      var index = 0
      var hash: Long = -1 // Danger Will Robinson
      if (length >= 32) {
        var v1 = seed + PRIME64_1 + PRIME64_2
        var v2 = seed + PRIME64_2
        var v3 = seed + 0
        var v4 = seed - PRIME64_1
        var limit = length - 32
        while (index <= limit){
          var k1 = (data(index).toLong & 0xff) |
            ((data(index + 1).toLong & 0xff) << 8) |
            ((data(index + 2) & 0xff) << 16) |
            ((data(index + 3) & 0xff) << 24) |
            ((data(index + 4) & 0xff) << 32) |
            ((data(index + 5) & 0xff) << 40) |
            ((data(index + 6) & 0xff) << 48) |
            ((data(index + 7) & 0xff) << 56) 
          v1 = mix(v1, k1)
          index += 8

          var k2 = (data(index) & 0xff) |
              ((data(index + 1) & 0xff) << 8)  |
              ((data(index + 2) & 0xff) << 16) |
              ((data(index + 3) & 0xff) << 24) |
              ((data(index + 4) & 0xff) << 32) |
              ((data(index + 5) & 0xff) << 40) |
              ((data(index + 6) & 0xff) << 48) |
              ((data(index + 7) & 0xff) << 56)
          v2 = mix(v2, k2)
          index += 8

          var k3 = (data(index) & 0xff) |
              ((data(index + 1) & 0xff) << 8)  |
              ((data(index + 2) & 0xff) << 16) |
              ((data(index + 3) & 0xff) << 24) |
              ((data(index + 4) & 0xff) << 32) |
              ((data(index + 5) & 0xff) << 40) |
              ((data(index + 6) & 0xff) << 48) |
              ((data(index + 7) & 0xff) << 56)
          v3 = mix(v3, k3)
          index += 8

          var k4 = (data(index) & 0xff) |
              ((data(index + 1) & 0xff) << 8)  |
              ((data(index + 2) & 0xff) << 16) |
              ((data(index + 3) & 0xff) << 24) |
              ((data(index + 4) & 0xff) << 32) |
              ((data(index + 5) & 0xff) << 40) |
              ((data(index + 6) & 0xff) << 48) |
              ((data(index + 7) & 0xff) << 56)
          v4 = mix(v4, k4)
          index += 8
        }

      hash = java.lang.Long.rotateLeft(v1, 1) + 
        java.lang.Long.rotateLeft(v2, 7) + 
        java.lang.Long.rotateLeft(v3, 12) +
        java.lang.Long.rotateLeft(v4, 18)

      hash = update(hash, v1)
      hash = update(hash, v2)
      hash = update(hash, v3)
      hash = update(hash, v4)
      } else {
        hash = seed + PRIME64_5
      }

      hash += length

      // tail
      while (index <= length - 8) {
        var tailStart: Int = index
        var k: Long = 0
        var remaining: Int = length - index
        remaining = if (remaining > 8) 8 else remaining
        remaining match {
          case 8 => 
            k |= (data(tailStart + 7) & 0xff) << 56
          case 7 =>
            k |= (data(tailStart + 6) & 0xff) << 48
          case 6 =>
            k |= (data(tailStart + 5) & 0xff) << 40
          case 5 =>
            k |= (data(tailStart + 4) & 0xff) << 32
          case 4 =>
            k |= (data(tailStart + 3) & 0xff) << 24
          case 3 =>
            k |= (data(tailStart + 2) & 0xff) << 16
          case 2 =>
            k |= (data(tailStart + 1) & 0xff) << 8
          case 1 =>
            k |= (data(tailStart) & 0xff)
        }
        hash = updateTail(hash, k)
        index += 8
      }

      if (index <= length - 4) {
        var tailStart = index
        var k = 0
        var remaining = length - index
        remaining = if (remaining > 4) 4 else remaining
        remaining match {
          case 4 =>
            k |= (data(tailStart + 3) & 0xff) << 24
          case 3 =>
            k |= (data(tailStart + 2) & 0xff) << 16
          case 2 =>
            k |= (data(tailStart + 1) & 0xff) << 8
          case 1 =>
            k |= (data(tailStart) & 0xff)
        }
        hash = updateTail(hash, k)
        index += 4
      }

      while (index < length) {
        hash = updateTail(hash, data(index))
        index += 1
      }

      hash = finalShuffle(hash)

      hash
    }


    private def mix(current: Long, value: Long): Long = {
      java.lang.Long.rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1
    }

    private def update(hash: Long, value: Long): Long ={
      val temp = hash ^ mix(0, value)
      temp * PRIME64_1 + PRIME64_4
    }

    private def updateTail(hash: Long, value: Long): Long ={
      val temp = hash ^ mix(0, value)
      java.lang.Long.rotateLeft(temp, 27) * PRIME64_1 + PRIME64_4
    }

    private def updateTail(hash: Long, value: Int): Long ={
      val unsigned = value & 0xFFFFFFFFL
      val temp = hash ^ (unsigned * PRIME64_1)
      java.lang.Long.rotateLeft(temp, 23) * PRIME64_2 + PRIME64_3
    }

    private def updateTail(hash: Long, value: Byte): Long ={
      var unsigned = value & 0xFF
      var temp = hash ^ (unsigned * PRIME64_5)
      java.lang.Long.rotateLeft(temp, 11) * PRIME64_1
    }

    private def finalShuffle(ihash: Long): Long = {
      var hash = ihash
      hash ^= hash >>> 33
      hash *= PRIME64_2
      hash ^= hash >>> 29
      hash *= PRIME64_3
      hash ^= hash >>> 32
      hash
    }
  }

}