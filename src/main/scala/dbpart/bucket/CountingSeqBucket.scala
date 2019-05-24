package dbpart.bucket
import dbpart._
import dbpart.shortread.Read
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

object CountingSeqBucket {
  val coverageCutoff = 5000.toShort

  def clipCov(cov: Long): Coverage = if (cov > coverageCutoff) coverageCutoff else cov.toShort

  def clipCov(cov: Int): Coverage = if (cov > coverageCutoff) coverageCutoff else cov.toShort

  def clipCov(cov: Short): Coverage = if (cov > coverageCutoff) coverageCutoff else cov

  def incrementCoverage(covSeq: Buffer[Coverage], pos: Int, amt: Coverage) = {
    covSeq.updated(pos, clipCov(covSeq(pos) + amt))
  }

  @volatile
  var mergeCount = 0
}

/**
 * A bucket that counts the coverage of each k-mer and represents them as joined sequences.
 */
abstract class CountingSeqBucket[+Self <: CountingSeqBucket[Self]](val sequences: Array[String],
  val coverages: Array[Array[Coverage]], val k: Int) extends CoverageBucket with Serializable {
  this: Self =>

  import CountingSeqBucket._

  def kmers = kmersBySequence.flatten
  def kmersBySequence = sequences.toSeq.map(Read.kmers(_, k))

  def numKmers = sequences.map(_.length() - (k-1)).sum

  def sequencesWithCoverage =
    sequences zip sequenceAvgCoverages

  def kmersWithCoverage =
    kmers.iterator zip kmerCoverages

  def kmersBySequenceWithCoverage =
    kmersBySequence zip sequenceCoverages

  /**
   * Produce a copy of this bucket with updated data.
   * @param sequencesUpdated whether sequence data has changed in the copy.
   */
  def copy(sequences: Array[String], coverage: Array[Array[Coverage]],
           sequencesUpdated: Boolean): Self

  /**
   * Produce a coverage-filtered version of this sequence bucket.
   * sequencesUpdated is initially set to false.
   */
  def atMinCoverage(minCoverage: Option[Coverage]): Self = {
    minCoverage match {
      case None => this
      case Some(cov) => coverageFilter(cov)
    }
  }

  def coverageFilter(cov: Int): Self = {
    var r: ArrayBuffer[String] = new ArrayBuffer(sequences.size)
    var covR: ArrayBuffer[Seq[Coverage]] = new ArrayBuffer(sequences.size)
    for {
      (s,c) <- (sequences zip coverages)
        filtered = coverageFilter(s, c, cov)
        (fs, fc) <- filtered
        if (fs.length > 0)
    } {
      r += fs
      covR += fc
    }
    copy(r.toArray, covR.map(_.toArray).toArray, false)
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
  def coverageFilter(seq: String, covs: Seq[Coverage], cov: Int): List[(String, Seq[Coverage])] = {
    if (covs.size == 0) {
      Nil
    } else {
      val dropKeepCov = covs.span(_ < cov)
      val keepNextCov = dropKeepCov._2.span(_ >= cov)
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

  /**
   * Find a k-mer in the bucket, incrementing its coverage.
   * @return true iff the sequence was found.
   */
  def findAndIncrement(data: String, inSeq: Seq[String],
                inCov: ArrayBuffer[Buffer[Coverage]], numSequences: Int,
                amount: Coverage = 1): Boolean = {
    var i = 0
    while (i < numSequences) {
      val s = inSeq(i)
      val index = s.indexOf(data)
      if (index != -1) {
        inCov(i) = incrementCoverage(inCov(i), index, amount)
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
                     intoCov: ArrayBuffer[Buffer[Coverage]], numSequences: Int): Int = {
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
//        if (mergeCount % 1000 == 0) {
//          println(s"$mergeCount merged sequences")
//        }
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
   * The sequence must be a k-mer.
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[String],
                     intoCov: ArrayBuffer[Buffer[Coverage]], numSequences: Int,
                     coverage: Coverage = 1): Int = {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < numSequences) {
      val existingSeq = intoSeq(i)
      if (existingSeq.startsWith(suffix)) {
        intoSeq(i) = (data.charAt(0) + existingSeq)
        intoCov(i) = clipCov(coverage) +: intoCov(i)

        //A merge is possible if a k-mer has both a prefix and a suffix match.
        //So it is sufficient to check for it here, as it would never hit the
        //append case below.
        return tryMerge(i, intoSeq, intoCov, numSequences)
      } else if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = (existingSeq + data.charAt(data.length() - 1))
        intoCov(i) = intoCov(i) :+ clipCov(coverage)
        return numSequences
      }
      i += 1
    }
    intoSeq += data
    intoCov += Buffer(clipCov(coverage))
    numSequences + 1
  }

  /**
   * Insert a number of k-mers, each with coverage 1.
   */
  def insertBulk(values: Iterable[String]): Option[Self] =
    Some(insertBulk(values, values.iterator.map(x => 1)))

  /**
   * Insert k-mers with corresponding coverages. Each value is a single k-mer.
   */
  def insertBulk(values: Iterable[String], coverages: Iterator[Coverage]): Self = {
    var seqR: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    var covR: ArrayBuffer[Buffer[Coverage]] = new ArrayBuffer(values.size + sequences.size)

    var sequencesUpdated = false
    var n = sequences.size
    seqR ++= sequences
    covR ++= this.coverages.map(_.toBuffer)

    for {
      (v, cov) <- values.iterator zip coverages
      if !findAndIncrement(v, seqR, covR, n, cov)
    } {
      n = insertSequence(v, seqR, covR, n, cov)
      sequencesUpdated = true
    }

    copy(seqR.toArray, covR.map(_.toArray).toArray, sequencesUpdated)
  }

  /**
   * Insert k-mer segments with corresponding coverages.
   * Each value is a segment with some number of overlapping k-mers. Each coverage item
   * applies to the whole of each such segments (all k-mers in a segment have the same coverage).
   *
   * This method is optimised for insertion of a larger number of distinct segments.
   */
  def insertBulkSegments(segmentsCoverages: Iterable[(String, Coverage)]): Self = {
    var r = this
    val insertAmt = segmentsCoverages.size

    var seqR: ArrayBuffer[String] = new ArrayBuffer(insertAmt + sequences.size)
    var covR: ArrayBuffer[Buffer[Coverage]] = new ArrayBuffer(insertAmt + sequences.size)
    var n = sequences.size
    seqR ++= sequences
    covR ++= this.coverages.map(_.toBuffer)

    for {
      (segment, cov) <- segmentsCoverages
      numKmers = segment.length() - (k - 1)
      kmer <- Read.kmers(segment, k).toSeq
      if !findAndIncrement(kmer, seqR, covR, n, cov)
    } {
      n = insertSequence(kmer, seqR, covR, n, cov)
    }

    copy(seqR.toArray, covR.map(_.toArray).toArray, true)
  }

  /**
   * Merge k-mers and coverages from another bucket into a new bucket.
   */
  def mergeBucket(other: CountingSeqBucket[_]): Self = {
    insertBulk(other.kmers, other.kmerCoverages)
  }
}

object SimpleCountingBucket {
  def empty(k: Int) = new SimpleCountingBucket(Array(), Array(), k)
}

final case class SimpleCountingBucket(override val sequences: Array[String],
  override val coverages: Array[Array[Coverage]],
  override val k: Int) extends CountingSeqBucket[SimpleCountingBucket](sequences, coverages, k)  {
  def copy(sequences: Array[String], coverage: Array[Array[Coverage]],
           sequencesUpdated: Boolean): SimpleCountingBucket =
        new SimpleCountingBucket(sequences, coverage, k)
}
