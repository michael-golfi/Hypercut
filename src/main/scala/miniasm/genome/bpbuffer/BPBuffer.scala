/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */
package miniasm.genome.bpbuffer
import java.nio.{ByteBuffer, ByteOrder, IntBuffer}
import java.util.Arrays

import miniasm.genome._

import scala.language.implicitConversions
import miniasm.genome.util.DNAHelpers

/**
 * The BPBuffer stores base pairs as "twobits", which are pairs of bits.
 * The actual data is stored in the Byte array "data". Offset is the offset into this array
 * where the BPs begin, where 0-3 are offsets in the first byte, 4-7 in the second, and so on.
 * Size is the number of BPs stored by this BPBuffer, starting from the offset.
 * The maximum storage capacity is achieved by offset = 0 and size = data.size * 4.
 *
 * BPBuffers are immutable. Instead of modifying the underlying data, modifying operations
 * create new instances.
 * Whenever possible we try to avoid copying the underlying byte array, changing instead the offset
 * and size parameters.
 *
 * The internal storage layout of data is continuous left-to-right:
 * (... ... bp1 bp2) (bp3 bp4 bp5 bp6) (bp7 bp8 bp9 bp10) (... ...
 * for offset = 2, size = 10, where the parentheses delimit each individual byte.
 */
trait BPBuffer {

  import BitRepresentation._

  /**
   * Number of BPs in this sequence.
   */
  def size: Short

  /**
   * Offset into the raw data array where this sequence starts (number of BPs).
   * Multiply by two to get bit offset.
   */
  def offset: Short

  /**
   * The raw data that backs this BPBuffer. This can contain more data than the sequence
   * itself, since it may be shared between multiple objects.
   * The sequence represented by a BPBuffer object will be a subsequence of the
   * one contained here.
   */
  def data: Array[Byte]

  /**
   * Obtain the (twobit) bp at a given offset.
   */
  def apply(pos: Int): Byte
  /**
   * Obtain the ACTG bp at a given offset.
   */
  def charAt(pos: Int): Char = twobitToChar(apply(pos))

  def bp(index: Int) = twobitToChar(apply(index))

  /**
   * Construct a new bpbuffer by prepending a new bp before the existing ones.
   */
  def prependTwobit(twobit: Byte): BPBuffer

  /**
   * Construct a new bpbuffer by appending a bp after the existing ones.
   */
  def appendTwobit(twobit: Byte): BPBuffer

  /**
   * Construct a new bpbuffer by removing bps from the start and returning the remainder.
   */
  def drop(size: Int): BPBuffer

  /**
   * Construct a new bpbuffer by taking a number of bps from the start.
   */
  def take(size: Int): BPBuffer

  /**
   * Construct a new bpbuffer from a subsequence of this one.
   */
  def slice(from: Short, length: Short): BPBuffer = BPBuffer.wrap(this, from, length)

  def kmers(k: Short): Iterator[BPBuffer] =
    (0 until (size - k + 1)).iterator.map(i => slice(i.toShort, k))

  def kmersAsLongArrays(k: Short): Iterator[Array[Long]] =
    (0 until (size - k + 1)).iterator.map(i => partAsLongArray(i.toShort, k))

  /**
   * Copy the raw data representing this sequence into a new integer array.
   */
  def asIntArray: Array[Int]

  /**
   * Copy the raw data representing part of this sequence into a new long array.
   */
  def partAsLongArray(offset: Short, size: Short): Array[Long]

  /**
   * Print detailed information about this object, including internal
   * storage and representation details.
   */
  def printDetailed(): Unit

  /**
   * Compare with another BPBuffer for ordering.
   */
  def compareWith(other: BPBuffer): Int

  def lessThan(other: BPBuffer): Boolean

  def reverseComplement: BPBuffer
}

/**
 * A BPBuffer is a sequence of base pairs. It is a low level building block to support
 * other classes in a storage-efficient way.
 *
 * @author Johan
 */

object BPBuffer {

  import BitRepresentation._

  implicit def toShort(i: Int) = i.toShort

  /**
   * Creates a new bpbuffer from an ACTG string.
   */
  def wrap(str: String): ZeroBPBuffer = {
    assert(str.length <= Short.MaxValue)
    new ZeroBPBuffer(stringToBytes(str), str.length)
  }

  /**
   * Creates a new bpbuffer from an ACTG string, with a given 0-based starting offset
   * and size.
   */
  def wrap(str: String, offset: Short, size: Short): ForwardBPBuffer = {
    new ForwardBPBuffer(stringToBytes(str), offset, size)
  }

  /**
   * Creates a new bpbuffer from an existing bpbuffer.
   * This saves memory by reusing the objects from the first one.
   * For instance, if the old one is ACTGACTG, and we do
   * BPBuffer.wrap(x,2,4), then the result contains TGAC, but using the same backing
   * buffer.
   */

  def wrap(buffer: BPBuffer, offset: Short, size: Short): BPBuffer = {
    if (size % 4 == 0) {
      assert(buffer.data.length >= size / 4)
    } else {
      assert(buffer.data.length >= size / 4 - 1)
    }

    assert(size <= (buffer.size - offset - buffer.offset))
    assert(size > 0 && offset >= 0)

    buffer match {
      case rev: RCBPBufferImpl => new RCBPBuffer(rev.data, rev.offset + (rev.size - size - offset), size)
      case fwd: BPBufferImpl   => new ForwardBPBuffer(fwd.data, fwd.offset + offset, size)
      case _                   => { throw new Exception("Unexpected buffer implementation") }
    }
  }

  def wrap(data: Int, offset: Short, size: Short): BPBuffer = {
    assert(offset + size <= 16)

    val bytes = Array[Byte](
      (data >> 24) & 0xff,
      (data >> 16).toByte & 0xff,
      (data >> 8).toByte & 0xff,
      data.toByte & 0xff)
    new ForwardBPBuffer(bytes, offset, size)
  }

  def wrap(data: Array[Int], offset: Short, size: Short): BPBuffer = {
    val bb = ByteBuffer.allocate(data.length * 4)
    var i = 0
    while (i < data.length) {
      bb.putInt(data(i))
      i += 1
    }
    new ForwardBPBuffer(bb.array(), offset, size)
  }

  def reverseComplement(buffer: BPBuffer) = {
    buffer match {
      case rev: RCBPBufferImpl => new ForwardBPBuffer(rev.data, rev.offset, rev.size)
      case fwd: BPBufferImpl   => new RCBPBuffer(fwd.data, fwd.offset, fwd.size)
      case _                   => { throw new Exception("Unexpected buffer implementation") }
    }
  }

  //Optimised version for repeated calls - avoids allocating a new buffer each time
  def longsToString(buffer: ByteBuffer, builder: StringBuilder, data: Array[Long], offset: Short, size: Short): String = {
    buffer.clear()
    builder.clear()
    var i = 0
    while (i < data.length) {
      buffer.putLong(data(i))
      i += 1
    }
    BitRepresentation.bytesToString(buffer.array(), builder, offset, size)
  }

  /**
   * One integer contains 4 bytes (16 bps). This function computes a single such integer from the
   * underlying backing buffer to achieve a simpler representation.
   * Main bottleneck in equality testing, hashing, ordering etc.
   */
  def computeIntArrayElement(data: Array[Byte], offset: Short, size: Short, i: Int): Int = {
    val os = offset
    val spo = size + os
    val shift = (os % 4) * 2
    val mask = (spo) % 4
    var res = 0

    val finalByte = (if (mask == 0) {
      (spo / 4) - 1
    } else {
      spo / 4
    })

    var pos = (os / 4) + i * 4
    var intshift = 32 + shift

    //adjust pos here, since some bytes may need to be used both for the final byte of one int
    //and for the first byte of the next
    pos -= 1

    var j = 0
    val dl = data.length
    while (j < 5 && pos + 1 <= finalByte) {
      //process 1 byte for each iteration

      intshift -= 8
      pos += 1

      var src = data(pos)

      //mask redundant bits from the final byte
      if (pos == finalByte) {
        if (mask == 1) {
          src = src & (0xfc << 4)
        } else if (mask == 2) {
          src = src & (0xfc << 2)
        } else if (mask == 3) {
          src = src & 0xfc
        }

      }
      var shifted = (src & 0xff)

      //adjust to position in the current int
      if (intshift > 0) {
        shifted = (shifted << intshift)
      } else {
        shifted = (shifted >> (-intshift))
      }

      //The bits should be mutually exclusive
      //				assert ((res & shifted) == 0)
      res |= shifted
      //				print(binint(newData(i)) + " ")

      j += 1
    }
    res
  }

  /**
   * The main implementation of the BPBuffer trait.
   */
  trait BPBufferImpl extends BPBuffer {

    import scala.language.implicitConversions
    /**
     * Returns a "twobit" in the form of a single byte.
     * Only the lowest two bits of the byte are valid. The others will be zeroed out.
     */
    final def directApply(pos: Int): Byte = {
      assert(pos >= 0 && pos < size)
      val truePos: Int = offset + pos
      val byte = truePos / 4
      val bval = data(byte)
      val localOffset = truePos % 4
      ((bval >> (2 * (3 - localOffset))) & 0x3)
    }

    def apply(pos: Int) = {
      directApply(pos)
    }

    final def asIntArray: Array[Int] = computeIntArray

    final def partAsIntArray(offset: Short, size: Short): Array[Int] = computeIntArray(offset, size)

    final def numInts = if (size % 16 == 0) { size >> 4 } else { (size >> 4) + 1 }

    /**
     * Create a new int array that contains the data in this bpbuffer, starting from offset 0.
     * This means that it's suitable for direct comparison and equality testing.
     * (BPBuffers may otherwise contain equivalent data, but in different backing buffers,
     * and starting from different offsets, so the backing buffers cannot easily be compared
     * directly for equality testing)
     */
    final def computeIntArray: Array[Int] = {
      var i = 0
      val ns = numInts
      val newData = new Array[Int](ns)
      while (i < ns) {
        newData(i) = computeIntArrayElement(i)
        i += 1
      }
      newData
    }

    /**
     * Create an int array representing a subsequence of this bpbuffer.
     */
    def computeIntArray(offset: Short, size: Short): Array[Int] = {
      var i = 0
      val ns = ((size >> 4) + 1) //safely (?) going slightly over in some cases
      val newData = new Array[Int](ns)
      while (i < ns) {
        newData(i) = BPBuffer.computeIntArrayElement(data, offset, size, i)
        i += 1
      }
      newData
    }

    protected def computeIntArrayElement(pos: Int): Int = {
      BPBuffer.computeIntArrayElement(data, offset, size, pos)
    }

    /**
     * Create a long array representing a subsequence of this bpbuffer.
     * @param offset
     * @param size
     * @return
     */
    final def partAsLongArray(offset: Short, size: Short): Array[Long] = {
      var write = 0
      var read = 0
      val numLongs = ((size >> 5) + 1) //safely (?) going slightly over in some cases
      val numInts = ((size >> 4) + 1)

      val newData = new Array[Long](numLongs)
      while (write < numLongs && read < numInts) {
        var x = BPBuffer.computeIntArrayElement(data, offset, size, read).toLong << 32
        read += 1
        if (read < numInts) {
          //Because this ends up as the second part of the long, we have to preserve the sign bit in its place.
          //Integer.toUnsignedLong will do the trick.
          x = (x | Integer.toUnsignedLong(BPBuffer.computeIntArrayElement(data, offset, size, read)))
          read += 1
        }
        newData(write) = x
        write += 1
      }
      newData
    }

    def binbyte(byte: Byte) = byteToQuad(byte)
    def binint(in: Int) = byteToQuad((in >> 24) & 0xff) +
      byteToQuad((in >> 16) & 0xff) + byteToQuad((in >> 8) & 0xff) +
      byteToQuad(in & 0xff) + "/" + java.lang.Integer.toString(in, 2)

    /**
     * Equality checking of two BPBufferImpl classes. Do they represent the same
     * sequence of BP's ?
     * As with kmerHashCode, the major source of complexity here is the fact that
     * equal kmers may have data starting at different offsets within an integer.
     * The computeIntArrayElement function compensates for this.
     */
    final def deepEquals(obfr: BPBufferImpl): Boolean = {
      if (size != obfr.size) {
        return false //try to fail fast
      } else {
        deepCompareSameSize(obfr) == 0
      }
    }

    final def deepCompareSameSize(obfr: BPBuffer): Int = {
      val a1 = asIntArray
      val a2 = obfr.asIntArray
      val n = a1.length //must be same for both if they have same size
      var i = 0
      while (i < n) {
        val x = a1(i)
        val y = a2(i)
        if (x < y) return -1
        if (x > y) return 1
        i += 1
      }
      0
    }

    /**
     * Hash a kmer.
     * This is a very frequent operation. Performance is crucial.
     * The main difficulty in hashing kmers stems from the fact that equal kmers
     * could have data that starts at different offsets, from the perspective of a single integer.
     * The data could start at any of the 16 bps that a 32-bit int can contain.
     * The computeIntArrayElement performs the necessary adjustments to make the data comparable.
     *
     * Note that we get a slight speedup if we make this function a lazy val, instead of a
     * def.
     * On the other hand, we use more memory that way.
     *
     */
    final def kmerHashCode: Int = {
      val n = numInts
      var r = 0
      var i = 0

      r = computeIntArrayElement(0)
      i += 1

      while (i < n) {
        //This is a common way to compute hash codes, multiply by 41 and add the next, spreads out the bits
        r = (r * 41) + computeIntArrayElement(i)
        i += 1
      }
      r
    }

    def toBPString: String = {
      bytesToString(data, new StringBuilder(size), offset, size)
    }

    override def toString(): String = toBPString

    //insert the twobit into orig.
    //offset is a 0-3 value.
    private def writeTwobitAtOffset(orig: Byte, twobit: Byte, offset: Int): Byte = {
      val r = orig & ~(3 << 2 * (3 - offset)) //turn off those bits we don't want
      val insertPart = (twobit << (2 * (3 - offset)))
      (r | insertPart).toByte
    }

    def appendTwobit(twobit: Byte) = appendTwobit(twobit, false)

    def appendTwobit(twobit: Byte, revCom: Boolean = false): BPBuffer = {
      //remaining BP positions in the final byte
      val remainingSpace = (4 - ((offset + size) % 4)) % 4

      //number of bytes for the new array
      var newSize = ((offset + size - 1) / 4) + 1
      var writeByte = 0

      if (remainingSpace > 0) {
        writeByte = newSize - 1
      } else {
        newSize += 1
        writeByte = newSize - 1
      }

      val newData = Arrays.copyOf(data, newSize)
      val writeOffset = ((offset + size) % 4)

      newData(writeByte) = writeTwobitAtOffset(newData(writeByte), twobit, writeOffset)

      if (revCom) {
        new RCBPBuffer(newData, offset, size + 1)
      } else {
        new ForwardBPBuffer(newData, offset, size + 1)
      }
    }

    def prependTwobit(twobit: Byte) = prependTwobit(twobit, false)

    def prependTwobit(twobit: Byte, revCom: Boolean = true): BPBuffer = {
      val newSize = size + 1
      val newData: Array[Byte] = Array.fill((newSize - 1) / 4 + 1)(0)
      newData(0) = (twobit << 6).toByte

      for (i <- 0 until size) {
        val writeByte = (i + 1) / 4
        val writeOffset = (i + 1) % 4

        val b = if (revCom) { directApply(i) } else { apply(i) }
        newData(writeByte) = writeTwobitAtOffset(newData(writeByte), b,
          writeOffset)
      }

      if (revCom) {
        new RCBPBuffer(newData, 0, newSize)
      } else {
        new ForwardBPBuffer(newData, 0, newSize)
      }
    }

    def drop(amt: Int): BPBuffer = {
      assert(amt < size)
      new ForwardBPBuffer(data, offset + amt, size - amt)
    }

    def take(amt: Int): BPBuffer = {
      assert(amt <= size)
      new ForwardBPBuffer(data, offset, amt)
    }

    def printDetailed() {
      println("BPBuffer size " + size + " offset " + offset + " " + getClass)
      print("Data ")
      data.foreach(x => { print(x + " ") })
      print(" Ints ")
      asIntArray.foreach(x => { print(x + " " + binint(x)) })
      println(" String " + toString)
    }

    /**
     * Compare with another BPBuffer for ordering.
     */
    final def compareWith(other: BPBuffer): Int = {
      if (size == other.size) {
        return deepCompareSameSize(other)
      }

      var i = 0

      while (i < size && i < other.size) {
        val x = apply(i)
        val y = other.apply(i)
        if (x < y) {
          return -1
        } else if (x > y) {
          return 1
        }
        i += 1
      }
      //unequal length but equal bps so far
      if (size > other.size) {
        return 1
      } else if (size < other.size) {
        return -1
      } else {
        //perfectly equal
        return 0
      }
    }

    final def lessThan(other: BPBuffer): Boolean = compareWith(other) < 0

    def reverseComplement(): BPBuffer = new RCBPBuffer(data, offset, size)
  }

  trait RCBPBufferImpl extends BPBufferImpl {
    //reverse complement position corresponding to the given position
    final def rcPos(pos: Int) = size - pos - 1

    override def apply(pos: Int): Byte = {
      assert(pos >= 0 && pos < size)
      complementOne(directApply(rcPos(pos)))
    }

    override def computeIntArrayElement(i: Int) = {
      val n = numInts
      val paddingBPs = (16 - (size % 16)) % 16 //number of AAA... BPs at the end of the last int in the forward repr.
      val doSnd = (n > 1 && i < n - 1)

      //read from two ints
      val fst = super.computeIntArrayElement(n - i - 1)
      val snd = if (doSnd) { super.computeIntArrayElement(n - i - 2) } else { 0 }

      //		  println(binint(fst))
      //		  println(binint(snd))

      var r = 0 // value we return
      var dest = 30 // position we write to in the current int (higher value means further to the left)

      //      println("fst")
      var it = paddingBPs * 2 //position we read from
      while (it < 32) {
        r = r | (complementOne(fst >>> it) << dest)
        it += 2
        dest -= 2
      }

      if (doSnd) {
        //        println("snd")
        it = 0
        while (it < paddingBPs * 2) {
          r = r | (complementOne(snd >>> it) << dest)
          it += 2
          dest -= 2
        }
      }
      r
    }

    override def computeIntArray(offset: Short, size: Short) = {
      //Subsequence computation not yet supported
      ???
    }

    override def toBPString: String = DNAHelpers.reverseComplement(bytesToString(data, new StringBuilder(size), offset, size))

    //not a bug! appending is implemented in terms of prepending to the reverse complement.
    override def appendTwobit(twobit: Byte): BPBuffer = prependTwobit(complementOne(twobit), true)
    //prepending is implemented in terms of "true" prepending.
    override def prependTwobit(twobit: Byte): BPBuffer = prependTwobit(twobit, false)

    override def drop(amt: Int): BPBuffer = {
      assert(amt < size)
      new RCBPBuffer(data, offset, size - amt)
    }

    override def take(amt: Int): BPBuffer = {
      assert(amt < size)
      new RCBPBuffer(data, offset + size - amt, amt)
    }

    override def reverseComplement: BPBuffer = new ForwardBPBuffer(data, offset, size)
  }

  /**
   * A reverse complement bpbuffer (the data is stored in reverse complement form).
   * Data will be read from right to left in the array, starting one step before the given offset,
   * and then complemented.
   */
  final case class RCBPBuffer(val data: Array[Byte], val offset: Short, val size: Short) extends RCBPBufferImpl {
    override def hashCode: Int = kmerHashCode
    override def equals(other: Any): Boolean = other match {
      case bbi: BPBufferImpl => deepEquals(bbi)
      case _                 => false
    }

  }

  final case class ForwardBPBuffer(val data: Array[Byte], val offset: Short, val size: Short) extends BPBufferImpl {
    override def hashCode: Int = kmerHashCode
    override def equals(other: Any): Boolean = other match {
      case bbi: BPBufferImpl => deepEquals(bbi)
      case _                 => false
    }
  }

  /**
   * A forward BPBuffer that has zero offset. Useful to save space in serialized encodings.
   */
  final case class ZeroBPBuffer(val data: Array[Byte], val size: Short) extends BPBufferImpl {
    def offset = 0

    override def hashCode: Int = kmerHashCode
    override def equals(other: Any): Boolean = other match {
      case bbi: BPBufferImpl => deepEquals(bbi)
      case _                 => false
    }
  }

}
