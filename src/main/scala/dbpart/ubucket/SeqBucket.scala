package dbpart.ubucket

import scala.collection.mutable.ArrayBuffer

trait Unpacker[B <: Bucket[_]] {
  def unpack(value: String, k: Int): B
}

trait Bucket[B <: Bucket[_]] {
  def pack: String
  def merge(other: B): B = ???
  def size: Int
  def items: Iterable[String]

  /**
   * Insert a single into the set, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertSingle(value: String): Option[B]

  /**
   * Insert a number of values into the set, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertBulk(values: Iterable[String]): Option[B]
}

object KmerBucket extends Unpacker[KmerBucket] {
  val separator: String = "\n"

  def unpack(set: String, k: Int): KmerBucket =
    new KmerBucket(set, set.split(separator, -1).toList, k)
}

final class KmerBucket(oldSet: String, val kmers: List[String], k: Int) extends Bucket[KmerBucket] {
  import KmerBucket._
  def size: Int = kmers.size
  def items = kmers

  /**
   * The default bucket implementation simply adds to a list.
   * Overriding methods can be more sophisticated, e.g. perform sorting or deduplication
   */
  def insertSingle(value: String): Option[KmerBucket] =
    Some(new KmerBucket(s"$oldSet$separator$value", value :: kmers, k))

  def insertBulk(values: Iterable[String]): Option[KmerBucket] = {
    val newVals = values.mkString(separator)
    Some(new KmerBucket(s"oldSet$separator$newVals", values.toList ::: kmers, k))
  }

  def pack: String = oldSet
}

object SeqBucket extends Unpacker[SeqBucket] {
  val separator: String = "\n"

  def unpack(value: String, k: Int): SeqBucket = {
    new SeqBucket(value.split(separator), k)
  }
}

/**
 * Bucket that merges sequences if possible.
 */
final class SeqBucket(val sequences: Iterable[String], k: Int) extends Bucket[SeqBucket] {
  import SeqBucket._

  def items = sequences

  def insertSingle(value: String): Option[SeqBucket] =
    insertBulk(Seq(value))

  def insertBulk(values: Iterable[String]): Option[SeqBucket] = {
    var r: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    r ++= sequences
    var updated = false
    for (v <- values; if !seqExists(v, r)) {
      insertSequence(v, r)
      updated = true
    }
    if (updated) {
      Some(new SeqBucket(r, k))
    } else {
      None
    }
  }

  def seqExists(data: String, in: Iterable[String]): Boolean = {
    val it = in.iterator
    while (it.hasNext) {
      val s = it.next
      if (s.indexOf(data) != -1) {
        return true
      }
    }
    false
  }

  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   */
  def insertSequence(data: String, into: ArrayBuffer[String]) {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < into.size) {
      val existingSeq = into(i)
      if (existingSeq.startsWith(suffix)) {
        into(i) = (data.charAt(0) + existingSeq)
        return
      }
      if (existingSeq.endsWith(prefix)) {
        into(i) = (existingSeq + data.charAt(data.length() - 1))
        return
      }
      i += 1
    }
    into += data
  }

  def pack: String = {
    sequences.mkString(separator)
  }

  def size: Int = {
    sequences.size
  }
}

