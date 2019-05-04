package dbpart.bucket
import dbpart._
import dbpart.shortread.Read
import scala.collection.mutable.ArrayBuffer

object CountingSeqBucket {
  val coverageCutoff = 5000

  def clipCov(cov: Long): Coverage = if (cov > coverageCutoff) coverageCutoff else cov.toInt

  def clipCov(cov: Int): Coverage = if (cov > coverageCutoff) coverageCutoff else cov

  def incrementCoverage(covSeq: Vector[Int], pos: Int, amt: Int) = {
    covSeq.updated(pos, clipCov(covSeq(pos) + amt))
  }

  @volatile
  var mergeCount = 0
}

/**
 * A bucket that counts the coverage of each k-mer and represents them as joined sequences.
 */
abstract class CountingSeqBucket[+Self <: CountingSeqBucket[Self]](val sequences: Array[String],
  val coverages: Seq[Array[Coverage]], val k: Int) extends CoverageBucket with Serializable {
  this: Self =>

  import CountingSeqBucket._

  def kmers = kmersBySequence.flatten
  def kmersBySequence = sequences.toSeq.map(Read.kmers(_, k))

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
  def copy(sequences: Array[String], coverage: Seq[Array[Coverage]],
           sequencesUpdated: Boolean): Self

  /**
   * Produce a coverage-filtered version of this sequence bucket.
   * sequencesUpdated is initially set to false.
   */
  def atMinCoverage(minCoverage: Option[Int]): Self = {
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
    copy(r.toArray, covR.map(_.toArray), false)
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
                inCov: ArrayBuffer[Vector[Coverage]], numSequences: Int,
                amount: Int = 1): Boolean = {
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
                     intoCov: ArrayBuffer[Vector[Coverage]], numSequences: Int): Int = {
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
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[String],
                     intoCov: ArrayBuffer[Vector[Coverage]], numSequences: Int,
                     coverage: Int = 1): Int = {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < numSequences) {
      val existingSeq = intoSeq(i)
      if (existingSeq.startsWith(suffix)) {
        intoSeq(i) = (data.charAt(0) + existingSeq)
        intoCov(i) = 1 +: intoCov(i)

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
    intoCov += Vector(clipCov(coverage))
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
    var r: ArrayBuffer[String] = new ArrayBuffer(values.size + sequences.size)
    var covR: ArrayBuffer[Vector[Coverage]] = new ArrayBuffer(values.size + sequences.size)

    var sequencesUpdated = false
    var n = sequences.size
    r ++= sequences
    covR ++= this.coverages.map(_.toVector)

    for {
      (v, cov) <- values.iterator zip coverages
      if !findAndIncrement(v, r, covR, n, cov)
    } {
      n = insertSequence(v, r, covR, n, cov)
      sequencesUpdated = true
    }

    copy(r.toArray, covR.map(_.toArray), sequencesUpdated)
  }

  /**
   * Insert k-mer segments with corresponding coverages.
   * Each value is a segment with some number of overlapping k-mers. Each coverage item
   * applies to the whole of each such segments (all k-mers in a segment have the same coverage).
   *
   * NB this implementation will cause a lot of temporary object allocations.
   * An expanded version would probably be more efficient.
   */
  def insertBulkSegments(values: Iterable[String], coverages: Iterable[Coverage]): Self = {
    var r = this
    for {
      (segment, cov) <- values.iterator zip coverages.iterator
      numKmers = segment.length() - (k - 1)
      segmentCoverages = (0 until numKmers).map(i => cov).iterator
      kmers = Read.kmers(segment, k).toSeq
    } {
      r = insertBulk(kmers, segmentCoverages)
    }
    r
  }

  /**
   * Merge k-mers and coverages from another bucket into a new bucket.
   */
  def mergeBucket(other: CountingSeqBucket[_]): Self = {
    insertBulk(other.kmers, other.kmerCoverages)
  }
}

object SimpleCountingBucket {
  def empty(k: Int) = new SimpleCountingBucket(Array(), Seq(), k)
}

final case class SimpleCountingBucket(override val sequences: Array[String],
  override val coverages: Seq[Array[Coverage]],
  override val k: Int) extends CountingSeqBucket[SimpleCountingBucket](sequences, coverages, k)  {
  def copy(sequences: Array[String], coverage: Seq[Array[Coverage]],
           sequencesUpdated: Boolean): SimpleCountingBucket =
        new SimpleCountingBucket(sequences, coverage, k)
}