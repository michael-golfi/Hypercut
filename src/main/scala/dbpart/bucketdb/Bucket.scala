package dbpart.bucketdb

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuilder
import java.util.Arrays
import dbpart.shortread.Read

trait Unpacker[Packed, B <: Bucket[Packed, B]] {
  def unpack(key: Packed, value: Packed, k: Int): B
}

trait Bucket[Packed, +B <: Bucket[Packed, B]] {
  def pack: Packed
  def size: Int
  def items: Iterable[Packed]

  /**
   * Insert a single into the bucket, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertSingle(value: Packed): Option[B]

  /**
   * Insert a number of values into the bucket, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertBulk(values: Iterable[Packed]): Option[B]
}

/**
 * Stores distinct strings (not necessarily NT sequences).
 * The value of k has no meaning for this implementation.
 */
object DistinctBucket extends Unpacker[String, DistinctBucket] {
  val separator: String = "\n"

  def unpack(key: String, set: String, k: Int): DistinctBucket =
    new DistinctBucket(set, set.split(separator, -1).toList)
}

final class DistinctBucket(oldSet: String, val items: List[String])
  extends Bucket[String, DistinctBucket] {
  import DistinctBucket._
  def size: Int = items.size

  def insertSingle(value: String): Option[DistinctBucket] = {
    if (!items.contains(value)) {
      Some(new DistinctBucket(s"$oldSet$separator$value", value :: items))
    } else {
      None
    }
  }

  def insertBulk(values: Iterable[String]): Option[DistinctBucket] = {
    val newVals = values.filter(!items.contains(_)).toList.distinct
    if (!newVals.isEmpty) {
      val newString = newVals.mkString(separator)
      Some(new DistinctBucket(s"$oldSet$separator$newString", newVals ::: items))
    } else {
      None
    }
  }

  def pack: String = oldSet
}


object DistinctByteBucket {
  type Rec = Array[Byte]

  class Unpacker(val itemSize: Int) extends dbpart.bucketdb.Unpacker[Array[Byte], DistinctByteBucket] {

    def unpack(key: Rec, set: Rec, k: Int): DistinctByteBucket = {
      var i = 0
      var items = List[Seq[Byte]]()
      while (i < set.length) {
        items ::= Arrays.copyOfRange(set, i, i + itemSize).toSeq
        i += itemSize
      }
      new DistinctByteBucket(set, items, itemSize)
    }
  }
}

/**
 * Distinct bucket for byte arrays.
 * Arrays do not have deep equality by default, so we also maintain them internally
 * as Seq[Byte] (which do).
 */
final class DistinctByteBucket(oldSet: Array[Byte], separated: List[Seq[Byte]],
  val itemSize: Int)
  extends Bucket[Array[Byte], DistinctByteBucket] {
  import DistinctByteBucket._

  def size = separated.size

  //Pad to size and also convert to Seq for equality
  def pad(value: Rec): Seq[Byte] = value.toSeq.padTo(itemSize, 0.toByte)

  def items: List[Rec] = separated.map(_.toArray)

  def insertSingle(value: Rec): Option[DistinctByteBucket] = {
    val pv = pad(value)
    if (!separated.contains(pv)) {
      Some(new DistinctByteBucket(oldSet ++ pv, pv :: separated, itemSize))
    } else {
      None
    }
  }

  def insertBulk(values: Iterable[Rec]): Option[DistinctByteBucket] = {
    val newVals = values.map(pad(_)).filter(!separated.contains(_)).toList.distinct
    if (!newVals.isEmpty) {
      val newData = oldSet ++ newVals.flatten
      Some(new DistinctByteBucket(newData, newVals ::: separated, itemSize))
    } else {
      None
    }
  }

  def pack: Rec = oldSet
}

/**
 * Bucket that merges sequences if possible.
 */
object SeqBucket extends Unpacker[String, SeqBucket] {
  val separator: String = "\n"

  def unpack(key: String, value: String, k: Int): SeqBucket = {
    new SeqBucket(value.split(separator), k)
  }
}

class SeqBucket(val sequences: Iterable[String], k: Int) extends Bucket[String, SeqBucket] {
  import SeqBucket._

  def items = sequences

  def kmers = kmersBySequence.flatten
  def kmersBySequence = items.map(Read.kmers(_, k))

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
   * (Note: depending on the order that sequences are seen, not every possible merge
   * will be carried out).
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

  @volatile
  var mergeCount = 0

  class Unpacker(dbLocation: String, minCoverage: Option[Int], buckets: Int)
    extends dbpart.bucketdb.Unpacker[String, CountingSeqBucket] {

    val covDB = new CoverageDB(dbLocation.replace(".kch", "_cov.kch"), buckets)

    def unpack(key: String, value: String, k: Int): CountingSeqBucket = {
      val cov = covDB.get(key)
      new CountingSeqBucket(value.split(separator, -1), cov, k).atMinCoverage(minCoverage)
    }
  }
}


/**
 * A bucket the counts the coverage of each k-mer, encoding it as a single char.
 */
final class CountingSeqBucket(sequences: Iterable[String],
  val coverage: CoverageBucket, k: Int, var sequencesUpdated: Boolean = false)
  extends SeqBucket(sequences, k) with Bucket[String, CountingSeqBucket] {

  import CountingSeqBucket._

  def sequencesWithCoverage =
    sequences zip coverage.sequenceAvgCoverages

  def kmersWithCoverage =
    kmers.iterator zip coverage.kmerCoverages

  def kmersBySequenceWithCoverage =
    kmersBySequence zip coverage.sequenceCoverages

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

  def kmerCoverages: Iterator[Int] = coverage.kmerCoverages

  override def insertSingle(value: String): Option[CountingSeqBucket] =
    insertBulk(Seq(value))

  /**
   * Find a k-mer in the bucket, incrementing its coverage.
   * @return true iff the sequence was found.
   */
  def findAndIncrement(data: String, inSeq: Seq[String],
                inCov: ArrayBuffer[String], numSequences: Int): Boolean = {
    var i = 0
    while (i < numSequences) {
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
   * Try to merge a pair of sequences that have a k-1 overlap.
   * @param atOffset use the prefix of this sequence as the basis for the merge.
   * @return
   */
  def tryMerge(atOffset: Int, intoSeq: ArrayBuffer[String],
                     intoCov: ArrayBuffer[String], numSequences: Int): Int = {
    val prefix = intoSeq(atOffset).substring(0, k - 1)
    var i = 0
    while (i < numSequences) {
      val existingSeq = intoSeq(i)
      if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = intoSeq(i).substring(0, intoSeq(i).length - (k - 1)) ++ intoSeq(atOffset)
        intoCov(i) = intoCov(i) ++ intoCov(atOffset)
        intoSeq.remove(atOffset)
        intoCov.remove(atOffset)
        mergeCount += 1
        if (mergeCount % 1000 == 0) {
          println(s"$mergeCount merged sequences")
        }
        return numSequences - 1
      }
      i += 1
    }
    //No merge
    numSequences
  }

  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   * Returns the new updated sequence count (may decrease due to merging).
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[String],
                     intoCov: ArrayBuffer[String], numSequences: Int): Int = {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < numSequences) {
      val existingSeq = intoSeq(i)
      if (existingSeq.startsWith(suffix)) {
        intoSeq(i) = (data.charAt(0) + existingSeq)
        intoCov(i) = asCoverage(1) + intoCov(i)

        //A merge is possible if a k-mer has both a prefix and a suffix match.
        //So it is sufficient to check for it here, as it would never hit the
        //append case below.
        return tryMerge(i, intoSeq, intoCov, numSequences)
      } else if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = (existingSeq + data.charAt(data.length() - 1))
        intoCov(i) = intoCov(i) + asCoverage(1)
        return numSequences
      }
      i += 1
    }
    intoSeq += data
    intoCov += asCoverage(1).toString()
    numSequences + 1
  }

  override def insertBulk(values: Iterable[String]): Option[CountingSeqBucket] = {
    var r: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    var covR: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)

    var n = sequences.size
    r ++= sequences
    covR ++= coverage.coverages

    for (v <- values; if !findAndIncrement(v, r, covR, n)) {
      n = insertSequence(v, r, covR, n)
      sequencesUpdated = true
    }

    Some(new CountingSeqBucket(r, new CoverageBucket(covR), k, sequencesUpdated))
  }
}