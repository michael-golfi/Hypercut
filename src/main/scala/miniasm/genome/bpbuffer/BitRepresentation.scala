/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */
package miniasm.genome.bpbuffer

import scala.collection.immutable._
import annotation.switch
import miniasm.genome._

/**
 * Helper functions for working with a low level bit representation of nucleotide sequences.
 * (Companion object)
 */
object BitRepresentation {
  val A: Byte = 0x0
  val C: Byte = 0x1
  val G: Byte = 0x2
  val T: Byte = 0x3

  val twobits = List(A, C, T, G)

  val byteToQuad = new Array[String](256)

  //  var quadToByte: Map[String, Byte] = new HashMap[String, Byte]()
  def quadToByte(quad: String) = quadToByteCompute(quad)

  //precompute conversion table once
  for (i <- 0 to 255) {
    val b = i.toByte
    val str = byteToQuadCompute(b)
    byteToQuad(b - Byte.MinValue) = str
    //    quadToByte += ((str, b))
  }

  /**
   * Convert a single BP from string representation to "twobit" representation.
   */
  def charToTwobit(char: Char): Byte = {
    (char: @switch) match {
      case 'A' => A
      case 'C' => C
      case 'G' => G
      case 'T' => T
    }
  }

  /**
   * Convert a single BP from twobit representation to string representation.
   */
  def twobitToChar(byte: Byte): Char = {
    (byte: @switch) match {
      case 0 => 'A'
      case 1 => 'C'
      case 2 => 'G'
      case 3 => 'T'
    }
  }

  /**
   * Convert a single byte to the "ACTG" format (a 4 letter string)
   */
  def byteToQuadCompute(byte: Byte): String = {
    var res = ""
    val chars = for (i <- 0 to 3) {
      val ptn = ((byte >> ((3 - i) * 2)) & 0x3)
      val char = twobitToChar(ptn.toByte)
      res += char
    }
    res
  }

  //If the string is too short, it will be padded on the right with 'A' (0).
  def quadToByteCompute(quad: String): Byte = {
    var res = 0
    var i = 0
    while (i < 4) {
      val c = if (i >= quad.length) 'A' else quad.charAt(i)
      val twobit = charToTwobit(c)
      if (i == 0) {
        res = twobit
      } else {
        res = (res << 2) | twobit
      }
      i += 1
    }
    res.toByte
  }

  /**
   * Convert a byte to a 4-character string.
   */
  def byteToQuad(byte: Byte): String = byteToQuad(byte - Byte.MinValue)

  import scala.language.implicitConversions

  implicit def toByte(int: Int) = int.toByte

  /**
   * Complement of a single BP.
   */
  def complementOne(byte: Byte) = complement(byte) & 0x3

  /**
   * Complement of a number of BPs packed in a byte.
   */
  def complement(byte: Byte) = {
    //	  println("Complement " + byte)
    (byte ^ 0xff).toByte
  }

  /**
   * Convert a 4-character string to a byte, starting from a given offset.
   */
  def quadToByte(str: String, offset: Int): Byte = {
    assert(str.size > 0)
    quadToByte(str.substring(offset))
  }

  /*
	 * Convert a string to an array of quads.
	 */
  def stringToBytes(bps: String): Array[Byte] = {
    var i = 0
    val rsize = (bps.size - 1) / 4
    val r = new Array[Byte](rsize + 1)
    while (i <= rsize) {
      //TODO keep passing a single string and pass the offset all the way down, instead of
      //generating intermediate substrings
      r(i) = quadToByte(bps, i * 4)
      i += 1
    }
    r
  }

  /**
   * Convert a byte array of quads to a string. The length of the
   * resulting string must be supplied.
   */
  def bytesToString(bytes: Array[Byte], offset: Int, size: Int): String = {
    val res = new StringBuilder()

    val startByte = offset / 4

    for (i <- startByte until bytes.size) {
      if (res.size < size) {
        if (i == startByte) {
          res.append(byteToQuad(bytes(i)).substring(offset % 4, 4))
        } else {
          res.append(byteToQuad(bytes(i)))
        }
      }

    }
    res.substring(0, size)
  }

  def continuations(km: BPBuffer) = twobits.map {
    km.drop(1).appendTwobit(_)
  }

  def continuesFrom(km: BPBuffer) = twobits.map {
    km.take(km.size - 1).prependTwobit(_)
  }

  def reverseComplement(bytes: Array[Byte], size: Int): Array[Byte] = {
    val r = Array.fill[Byte](size)(0)
    var i = 0
    while (i < size) {
      var j = 0
      var b: Byte = 0
      while (j < 4) {
        val mask: Byte = 0xf >> (3 - j)
        b |= complement((bytes(size - i) & mask))
        j += 1
      }
      r(i) = b
      i += 1
    }
    r
  }

}
