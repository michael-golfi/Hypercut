package dbpart.ubucket

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

trait Unpacker[B <: Bucket[B]] {
  def unpack(value: String, k: Int): B
}

trait Bucket[+B <: Bucket[_]] {
  def pack: String
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
class SeqBucket(val sequences: Iterable[String], k: Int) extends Bucket[SeqBucket] {
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

/**
 * A sequence bucket that merges sequences and tracks the coverage
 * of each k-mer.
 * Coverages are tracked similar to phred scores, with a single ascii char
 * for each k-mer. They are clipped at a maximum bound.
 */
object CountingSeqBucket extends Unpacker[CountingSeqBucket] {
  val separator: String = SeqBucket.separator

  val zeroCoverage = '0'
  val coverageCutoff = 150
  val maxCoverage = (zeroCoverage + coverageCutoff).toChar

  def asCoverage(cov: Int): Char = {
    if (cov > maxCoverage) asCoverage(maxCoverage) else {
      (cov + zeroCoverage).toChar
    }
  }

  def covToInt(cov: Char): Int = {
    (cov - zeroCoverage)
  }

  def clip(cov: Char) = if (cov > maxCoverage) maxCoverage else cov

  def incrementCoverage(covString: String, pos: Int, amt: Int) = {
    val pre = covString.substring(0, pos)
    val nc = clip((covString.charAt(pos) + amt).toChar)
    val post = covString.substring(pos + 1)
    pre + nc + post
  }

  @tailrec
  def unpack(from: Iterator[String], k: Int, buildingSeq: List[String] = Nil,
             buildingCov: List[String] = Nil): CountingSeqBucket = {
    if (from.hasNext) {
      val s = from.next
      val c = from.next
      unpack(from, k, s:: buildingSeq, c:: buildingCov)
    } else {
      new CountingSeqBucket(buildingSeq, buildingCov, k)
    }
  }

  def unpack(value: String, k: Int): CountingSeqBucket = {
    unpack(value.split(separator, -1).iterator, k)
  }

}

final class CountingSeqBucket(sequences: Iterable[String],
  coverage: Iterable[String], k: Int) extends SeqBucket(sequences, k) with Bucket[CountingSeqBucket] {
  import CountingSeqBucket._

  def kmerCoverages: Iterable[Int] = coverage.flatMap(_.map(covToInt))

  override def pack: String = {
    (sequences zip coverage).map(sc => s"${sc._1}$separator${sc._2}").mkString(separator)
  }

  override def insertSingle(value: String): Option[CountingSeqBucket] =
    insertBulk(Seq(value))


  def findAndIncrement(data: String, inSeq: Seq[String],
                inCov: ArrayBuffer[String]): Boolean = {
    var i = 0
    while (i < inSeq.size) {
      val s = inSeq(i)
      val index = s.indexOf(data)
      if (index != -1) {
        inCov(i) = incrementCoverage(inCov(i), index, 1)
        return true
      }
      i += 1
    }
    false
  }

  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[String], intoCov: ArrayBuffer[String]) {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < intoSeq.size) {
      val existingSeq = intoSeq(i)
      if (existingSeq.startsWith(suffix)) {
        intoSeq(i) = (data.charAt(0) + existingSeq)
        intoCov(i) = asCoverage(1) + intoCov(i)
        return
      }
      if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = (existingSeq + data.charAt(data.length() - 1))
        intoCov(i) = intoCov(i) + asCoverage(1)
        return
      }
      i += 1
    }
    intoSeq += data
    intoCov += asCoverage(1).toString()
  }

  override def insertBulk(values: Iterable[String]): Option[CountingSeqBucket] = {
    var r: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    var covR: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    r ++= sequences
    covR ++= coverage

    for (v <- values; if !findAndIncrement(v, r, covR)) {
      insertSequence(v, r, covR)
    }

    Some(new CountingSeqBucket(r, covR, k))
  }
}

