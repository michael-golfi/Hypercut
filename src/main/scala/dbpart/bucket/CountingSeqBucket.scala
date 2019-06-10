package dbpart.bucket
import dbpart._
import dbpart.shortread.Read
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MMap}

object CountingSeqBucket {
  //The maximum abundance value that we track. Currently this is an ad hoc limit.
  val abundanceCutoff = 5000.toShort

  //At more than this many sequences, we build a map to speed up the insertion process.
  val helperMapThreshold = 16

  //If the number of sequences goes above this limit, we emit a warning.
  val warnBucketSize = 1000

  def clipAbundance(abund: Long): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund.toShort

  def clipAbundance(abund: Int): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund.toShort

  def clipAbundance(abund: Short): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund

  def incrementAbundance(abundSeq: Buffer[Abundance], pos: Int, amt: Abundance) = {
    abundSeq(pos) = clipAbundance(abundSeq(pos) + amt)
  }

  @volatile
  var mergeCount = 0
}

/**
 * A bucket that counts the abundance of each k-mer and represents them as joined sequences.
 */
abstract class CountingSeqBucket[+Self <: CountingSeqBucket[Self]](val sequences: Array[String],
  val abundances: Array[Array[Abundance]], val k: Int) extends AbundanceBucket with Serializable {
  this: Self =>

  import CountingSeqBucket._

  def kmers = kmersBySequence.flatten
  def kmersBySequence = sequences.toSeq.map(Read.kmers(_, k))

  def numKmers = sequences.map(_.length() - (k-1)).sum

  def sequencesWithAbundance =
    sequences zip sequenceAvgAbundances

  def kmersWithAbundance =
    kmers.iterator zip kmerAbundances

  def kmersBySequenceWithAbundance =
    kmersBySequence zip sequenceAbundances

  /**
   * Produce a copy of this bucket with updated data.
   * @param sequencesUpdated whether sequence data has changed in the copy.
   */
  def copy(sequences: Array[String], abundance: Array[Array[Abundance]],
           sequencesUpdated: Boolean): Self

  /**
   * Produce an abundance-filtered version of this sequence bucket.
   * sequencesUpdated is initially set to false.
   */
  def atMinAbundance(minAbundance: Option[Abundance]): Self = {
    minAbundance match {
      case None => this
      case Some(abund) => abundanceFilter(abund)
    }
  }

  def abundanceFilter(abund: Int): Self = {
    var r: ArrayBuffer[String] = new ArrayBuffer(sequences.size)
    var abundR: ArrayBuffer[Seq[Abundance]] = new ArrayBuffer(sequences.size)
    for {
      (s,c) <- (sequences zip abundances)
        filtered = abundanceFilter(s, c, abund)
        (fs, fc) <- filtered
        if (fs.length > 0)
    } {
      r += fs
      abundR += fc
    }
    copy(r.toArray, abundR.map(_.toArray).toArray, false)
  }

  /**
   * Filter a single contiguous sequence. Since it may be split as a result of
   * abundance filtering, the result is a list of sequences.
   * The result may contain empty sequences.
   *
   * Example sequence assuming k=4:
   * ACTGGTG
   * Abundance
   * 3313
   * Cut at k=3
   * The result splits the sequence and should then be:
   * ACTGG
   * 33
   * GGTG
   * 3
   */
  def abundanceFilter(seq: String, abunds: Seq[Abundance], abund: Int): List[(String, Seq[Abundance])] = {
    if (abunds.size == 0) {
      Nil
    } else {
      val dropKeepAbund = abunds.span(_ < abund)
      val keepNextAbund = dropKeepAbund._2.span(_ >= abund)
      val droppedLength = dropKeepAbund._1.length
      val keptLength = keepNextAbund._1.length
      val keepSeq = if (keptLength > 0) {
        seq.substring(droppedLength, droppedLength + keptLength + k - 1)
      } else {
        ""
      }
      (keepSeq, keepNextAbund._1) :: abundanceFilter(seq.substring(droppedLength + keptLength),
        keepNextAbund._2, abund)
    }
  }

  /**
   * Find a k-mer in the bucket, incrementing its abundance.
   * @return true iff the sequence was found.
   */
  def findAndIncrement(data: String, inSeq: Seq[StringBuilder],
                inAbund: ArrayBuffer[Buffer[Abundance]], numSequences: Int,
                amount: Abundance = 1): Boolean = {
    helperMap match {
      case Some(m) =>
        m.get(data) match {
          case Some((i, j)) =>
            incrementAbundance(inAbund(i), j, amount)
            return true
          case None =>
            return false
        }
      case _ =>
    }

    var i = 0
    while (i < numSequences) {
      val s = inSeq(i)
      val index = s.indexOf(data)
      if (index != -1) {
        incrementAbundance(inAbund(i), index, amount)
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
  def tryMerge(atOffset: Int, intoSeq: ArrayBuffer[StringBuilder],
                     intoAbund: ArrayBuffer[Buffer[Abundance]], numSequences: Int): Int = {
    val prefix = intoSeq(atOffset).substring(0, k - 1)
    var i = 0
    while (i < numSequences && i != atOffset) {
      val existingSeq = intoSeq(i)
      if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = intoSeq(atOffset).insert(0,
            intoSeq(i).substring(0, intoSeq(i).length - (k - 1)))
        intoAbund(i) ++= intoAbund(atOffset)
        intoSeq.remove(atOffset)
        intoAbund.remove(atOffset)

        mergeCount += 1
//        if (mergeCount % 1000 == 0) {
//          println(s"$mergeCount merged sequences")
//        }

        //Various sequences and their offsets may have changed
        if (helperMap != None) {
          initHelperMap()
        }
        return numSequences - 1
      }
      i += 1
    }

    //No merge
    updateHelperMap(intoSeq(atOffset), atOffset)
    numSequences
  }

  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   * Returns the new updated sequence count (may decrease due to merging).
   * The sequence must be a k-mer.
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[StringBuilder],
                     intoAbund: ArrayBuffer[Buffer[Abundance]], numSequences: Int,
                     abundance: Abundance = 1): Int = {
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < numSequences) {
      val existingSeq = intoSeq(i)
      if (existingSeq.startsWith(suffix)) {
        intoSeq(i) = existingSeq.insert(0, data.charAt(0))
        intoAbund(i).insert(0, clipAbundance(abundance))

        //A merge is possible if a k-mer has both a prefix and a suffix match.
        //So it is sufficient to check for it here, as it would never hit the
        //append case below.
        return tryMerge(i, intoSeq, intoAbund, numSequences)
      } else if (existingSeq.endsWith(prefix)) {
        intoSeq(i) = (existingSeq += data.charAt(data.length() - 1))
        intoAbund(i) += clipAbundance(abundance)
        updateHelperMap(intoSeq(i), i)
        return numSequences
      }
      i += 1
    }
    intoSeq += new StringBuilder(data)
    updateHelperMap(intoSeq(numSequences), numSequences)
    intoAbund += Buffer(clipAbundance(abundance))
    numSequences + 1
  }

  /**
   * Insert a number of k-mers, each with abundance 1.
   */
  def insertBulk(values: Iterable[String]): Option[Self] =
    Some(insertBulk(values, values.iterator.map(x => 1)))

  /**
   * Insert k-mers with corresponding abundances. Each value is a single k-mer.
   */
  def insertBulk(values: Iterable[String], abundances: Iterator[Abundance]): Self = {
    var seqR: ArrayBuffer[StringBuilder] = new ArrayBuffer(values.size + sequences.size)
    var abundR: ArrayBuffer[Buffer[Abundance]] = new ArrayBuffer(values.size + sequences.size)

    var sequencesUpdated = false
    var n = sequences.size
    seqR ++= sequences.iterator.map(s => new StringBuilder(s))
    abundR ++= this.abundances.map(_.toBuffer)

    for {
      (v, abund) <- values.iterator zip abundances
      if !findAndIncrement(v, seqR, abundR, n, abund)
    } {
      n = insertSequence(v, seqR, abundR, n, abund)
      sequencesUpdated = true
    }

    copy(seqR.map(_.toString).toArray, abundR.map(_.toArray).toArray, sequencesUpdated)
  }

  /**
   * Insert k-mer segments with corresponding abundances.
   * Each value is a segment with some number of overlapping k-mers. Each abundance item
   * applies to the whole of each such segments (all k-mers in a segment have the same abundance).
   *
   * This method is optimised for insertion of a larger number of distinct segments.
   */
  def insertBulkSegments(segmentsAbundances: Iterable[(String, Abundance)]): Self = {
    var r = this
    val insertAmt = segmentsAbundances.size

    val bufSize = insertAmt + sequences.size
    var seqR: ArrayBuffer[StringBuilder] = new ArrayBuffer(bufSize)
    var abundR: ArrayBuffer[Buffer[Abundance]] = new ArrayBuffer(bufSize)
    if (bufSize > helperMapThreshold) {
     initHelperMap()
    }

    var n = sequences.size
    seqR ++= sequences.map(x => new StringBuilder(x))
    abundR ++= this.abundances.map(_.toBuffer)

    for {
      (segment, abund) <- segmentsAbundances
      numKmers = segment.length() - (k - 1)
      kmer <- Read.kmers(segment, k).toSeq
      if !findAndIncrement(kmer, seqR, abundR, n, abund)
    } {
      n = insertSequence(kmer, seqR, abundR, n, abund)
    }

    if (n >= warnBucketSize) {
      Console.err.println(s"WARNING: bucket of size $n, sample sequences:")
      Console.err.println(seqR.take(10).mkString(" "))
    }

    copy(seqR.map(_.toString).toArray,
      abundR.map(_.toArray).toArray, true)
  }

  /**
   * Merge k-mers and abundances from another bucket into a new bucket.
   */
  def mergeBucket(other: CountingSeqBucket[_]): Self = {
    insertBulk(other.kmers, other.kmerAbundances)
  }

  //If assigned, maps k-mers to their sequence and position in that sequence.
  @transient
  var helperMap: Option[MMap[String, (Int, Int)]] = None

  private def initHelperMap() {
    val r = MMap[NTSeq, (Int, Int)]()
    for {
      (seq, seqIdx) <- kmersBySequence.zipWithIndex
      (kmer, kmerIdx) <- seq.zipWithIndex
    } {
      r += (kmer -> (seqIdx, kmerIdx))
    }
    helperMap = Some(r)
  }

  private def updateHelperMap(sequence: StringBuilder, i: Int) {
    updateHelperMap(sequence.toString, i)
  }

  private def updateHelperMap(sequence: NTSeq, i: Int) {
    helperMap match {
      case Some(m) =>
        for {
          (kmer, j) <- Read.kmers(sequence, k).zipWithIndex
        } {
          m += (kmer -> (i, j))
        }
      case _ =>
    }
  }
}

object SimpleCountingBucket {
  def empty(k: Int) = new SimpleCountingBucket(Array(), Array(), k)
}

final case class SimpleCountingBucket(override val sequences: Array[String],
  override val abundances: Array[Array[Abundance]],
  override val k: Int) extends CountingSeqBucket[SimpleCountingBucket](sequences, abundances, k)  {
  def copy(sequences: Array[String], abundances: Array[Array[Abundance]],
           sequencesUpdated: Boolean): SimpleCountingBucket =
        new SimpleCountingBucket(sequences, abundances, k)
}
