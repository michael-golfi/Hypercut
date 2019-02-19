package dbpart.ubucket

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

trait Unpacker[B <: Bucket[B]] {
  def unpack(key: String, value: String, k: Int): B
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

  def unpack(key: String, set: String, k: Int): KmerBucket =
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

  def unpack(key: String, value: String, k: Int): SeqBucket = {
    new SeqBucket(value.split(separator), k)
  }
}

/**
 * Bucket that merges sequences if possible.
 */
class SeqBucket(val sequences: Iterable[String], k: Int) extends Bucket[SeqBucket] {
  import SeqBucket._

  def items = sequences

  def kmers = items.flatMap(_.sliding(k))

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

  /**
   * Distinct k-1 length beginnings of sequences in this bucket
   */
  def heads: Iterable[String] =
    sequences.map(_.take(k-1)).toSeq.distinct

  /**
   * Distinct k-1 length endings of sequences in this bucket
   */
  def tails: Iterable[String] =
    sequences.map(_.takeRight(k-1)).toSeq.distinct
}

/**
 * A sequence bucket that merges sequences and tracks the coverage
 * of each k-mer.
 * Coverages are tracked similar to phred scores, with a single unicode char
 * for each k-mer. They are clipped at a maximum bound.
 */
object CountingSeqBucket {
  val separator: String = SeqBucket.separator

  val zeroCoverage = '0'
  val coverageCutoff = 5000
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

}

class CountingUnpacker(dbLocation: String, minCoverage: Option[Int]) extends Unpacker[CountingSeqBucket] {
  import CountingSeqBucket._

  val covDB = new CoverageDB(dbLocation.replace(".kch", "_cov.kch"))

  def unpack(key: String, value: String, k: Int): CountingSeqBucket = {
    val cov = covDB.get(key)
    new CountingSeqBucket(value.split(separator, -1), cov, k).atMinCoverage(minCoverage)
  }
}

final class CountingSeqBucket(sequences: Iterable[String],
  val coverage: CoverageBucket, k: Int,
  var sequencesUpdated: Boolean = false) extends SeqBucket(sequences, k) with Bucket[CountingSeqBucket] {
  import CountingSeqBucket._

  def sequencesWithCoverage =
    sequences zip coverage.sequenceAvgCoverages

  /**
   * Produce a coverage-filtered version of this sequence bucket.
   * sequencesUpdated is initially set to false.
   */
  def atMinCoverage(minCoverage: Option[Int]): CountingSeqBucket = {
    minCoverage match {
      case None => this
      case Some(cov) => coverageFilter(cov)
    }
  }

  def coverageFilter(cov: Int): CountingSeqBucket = {
    var r: ArrayBuffer[String] = new ArrayBuffer(sequences.size)
    var covR: ArrayBuffer[String] = new ArrayBuffer(sequences.size)
    for {
      (s,c) <- (sequences zip coverage.coverages)
        filtered = coverageFilter(s, c, cov)
        (fs, fc) <- filtered
        if (fs.length > 0)
    } {
      r += fs
      covR += fc
    }
    new CountingSeqBucket(r, new CoverageBucket(covR), k, false)
  }

  /**
   * Filter a single contiguous sequence. Since it may be split as a result of
   * coverage filtering, the result is a list of sequences.
   * The result may contain empty sequences.
   *
   * Example sequence assuming k=4:
   * ACTGGTG
   * Coverage:
   * 3313
   * Cut at k=3
   * The result splits the sequence and should then be:
   * ACTGG
   * 33
   * GGTG
   * 3
   */
  def coverageFilter(seq: String, covs: String, cov: Int): List[(String, String)] = {
    if (covs.length() == 0) {
      Nil
    } else {
      val dropKeepCov = covs.span(covToInt(_) < cov)
      val keepNextCov = dropKeepCov._2.span(covToInt(_) >= cov)
      val droppedLength = dropKeepCov._1.length
      val keptLength = keepNextCov._1.length
      val keepSeq = if (keptLength > 0) {
        seq.substring(droppedLength, droppedLength + keptLength + k - 1)
      } else {
        ""
      }
      (keepSeq, keepNextCov._1) :: coverageFilter(seq.substring(droppedLength + keptLength), keepNextCov._2, cov)
    }
  }

  def kmerCoverages: Iterable[Int] = coverage.kmerCoverages

  override def insertSingle(value: String): Option[CountingSeqBucket] =
    insertBulk(Seq(value))

  /**
   * Find a k-mer in the bucket, incrementing its coverage.
   * @return true iff the sequence was found.
   */
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
    covR ++= coverage.coverages

    for (v <- values; if !findAndIncrement(v, r, covR)) {
      insertSequence(v, r, covR)
      sequencesUpdated = true
    }

    Some(new CountingSeqBucket(r, new CoverageBucket(covR), k, sequencesUpdated))
  }
}
