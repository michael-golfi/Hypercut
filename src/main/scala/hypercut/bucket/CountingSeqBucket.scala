package hypercut.bucket
import hypercut._
import hypercut.shortread.Read
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.IndexedSeq
import scala.collection.mutable.{Map => MMap}
import miniasm.genome.util.DNAHelpers
import hypercut.graph.PathGraphBuilder
import hypercut.graph.PathFinder
import hypercut.graph.KmerNode

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

  def incrementAbundance(abundSeq: IndexedSeq[Abundance], pos: Int, amt: Abundance) = {
    abundSeq(pos) = clipAbundance(abundSeq(pos) + amt)
  }

  @volatile
  var mergeCount = 0
}

case class BucketStats(sequences: Int, totalAbundance: Int, kmers: Int)

/**
 * A bucket that counts the abundance of each k-mer and represents them as joined sequences.
 */
abstract class CountingSeqBucket[+Self <: CountingSeqBucket[Self]](val sequences: Array[String],
  val abundances: Array[Array[Abundance]], val k: Int) extends AbundanceBucket with Serializable {
  this: Self =>

  import CountingSeqBucket._

  def kmers = kmersBySequence.flatten
  def kmersBySequence = sequences.toSeq.map(Read.kmers(_, k))
  def suffixes = kmers.map(DNAHelpers.kmerSuffix(_, k))
  def prefixes = kmers.map(DNAHelpers.kmerPrefix(_, k))

  def numKmers = sequences.map(_.length() - (k-1)).sum

  def stats = new BucketStats(sequences.length, abundances.flatten.map(_.toInt).sum, numKmers)

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
    val r: ArrayBuffer[String] = new ArrayBuffer(sequences.size)
    val abundR: ArrayBuffer[Seq[Abundance]] = new ArrayBuffer(sequences.size)
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
    if (abunds.isEmpty) {
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
  def findAndIncrement(data: String, inSeq: IndexedSeq[StringBuilder],
                inAbund: IndexedSeq[ArrayBuffer[Abundance]], numSequences: Int,
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
      if (s != null) {
        val index = s.indexOf(data)
        if (index != -1) {
          incrementAbundance(inAbund(i), index, amount)
          return true
        }
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
                     intoAbund: ArrayBuffer[ArrayBuffer[Abundance]], numSequences: Int) {
    val prefix = intoSeq(atOffset).substring(0, k - 1)
    var i = 0
    var failedToFind = false

    /*
     * Traversing the entire list of sequences would cause a n^2 cost for large buckets,
     * so if possible we use the helper maps to avoid the traversal.
     */
    helperMap match {
      case Some(m) =>
        suffixHelper.get(prefix) match {
          case Some(j) =>
            if (j == atOffset) {
              //Many sequences could have the same suffix, but only
              //one of them is tracked by the helper map.
              //Self merge (with atOffset) is not allowed.
              //Force full traversal in this case.
              i = 0
            } else {
              i = j
            }
          case _       => failedToFind = true
        }
      case _ =>
    }

    while (i < numSequences && i != atOffset && !failedToFind) {
      val existingSeq = intoSeq(i)
      if (existingSeq != null) {
        if (existingSeq.endsWith(prefix)) {
          intoSeq(i) = intoSeq(atOffset).insert(
            0,
            intoSeq(i).substring(0, intoSeq(i).length - (k - 1)))
          intoAbund(i) ++= intoAbund(atOffset)

          mergeCount += 1
          if (helperMap != None) {
            removeHelperSuffix(existingSeq)
            removeHelperPrefix(intoSeq(atOffset))
            updateHelperMaps(intoSeq(i), i)
          }

          //Replace the old StringBuilder with null so that
          //helper maps can mostly remain valid without rebuilding
          //(offsets of untouched StringBuilders do not change)
          intoSeq(atOffset) = null
          intoAbund(atOffset) = null
          return
        }
      }
      i += 1
    }

    //No merge
    updateHelperMaps(intoSeq(atOffset), atOffset)
  }

  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   * Returns the new updated sequence count (may decrease due to merging).
   * The sequence must be a k-mer.
   */
  def insertSequence(data: String, intoSeq: ArrayBuffer[StringBuilder],
                     intoAbund: ArrayBuffer[ArrayBuffer[Abundance]], numSequences: Int,
                     abundance: Abundance = 1): Int = {
    val suffix = DNAHelpers.kmerSuffix(data, k)
    val prefix = DNAHelpers.kmerPrefix(data, k)
    var i = 0
    var noPrefix = false
    var noSuffix = false

    /*
     * Traversing the entire list of sequences would cause a n^2 cost for large buckets,
     * so if possible we use the helper maps to avoid the traversal.
     */
    helperMap match {
      case Some(m) =>
        prefixHelper.get(suffix) match {
          case Some(pi) =>
            i = pi
          case None =>
            noPrefix = true
            suffixHelper.get(prefix) match {
              case Some(si) => i = si
              case _        => noSuffix = true
            }
        }

      case None =>
    }

    //Look for a prefix match
    while (i < numSequences && !noPrefix) {
      val existingSeq = intoSeq(i)
      if (existingSeq != null) {
        if (DNAHelpers.kmerPrefix(existingSeq, k) == suffix) {
          removeHelperPrefix(existingSeq)
          existingSeq.insert(0, data.charAt(0))
          intoAbund(i).insert(0, clipAbundance(abundance))

          //A merge is possible if a k-mer has both a prefix and a suffix match.
          //So it is sufficient to check for it here, as it would never hit the
          //append case below.
          tryMerge(i, intoSeq, intoAbund, numSequences)
          return numSequences
        }
      }
      i += 1
    }

    if (i == numSequences) i = 0

    //Look for a suffix match
    while (i < numSequences && !noSuffix) {
      val existingSeq = intoSeq(i)
      if (existingSeq != null) {
        if (DNAHelpers.kmerSuffix(existingSeq, k) == prefix) {
          removeHelperSuffix(existingSeq)
          existingSeq += data.charAt(data.length() - 1)
          intoAbund(i) += clipAbundance(abundance)
          updateHelperMaps(intoSeq(i), i)
          return numSequences
        }
      }
      i += 1
    }

    //Note: could possibly be more efficient by looking for
    //positions that have been set to null following a merge,
    //and reusing those positions
    intoSeq += new StringBuilder(data)
    updateHelperMaps(intoSeq(numSequences), numSequences)
    intoAbund += ArrayBuffer(clipAbundance(abundance))
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
    val seqR: ArrayBuffer[StringBuilder] = new ArrayBuffer(values.size + sequences.size)
    val abundR: ArrayBuffer[ArrayBuffer[Abundance]] = new ArrayBuffer(values.size + sequences.size)

    var sequencesUpdated = false
    var n = sequences.size
    seqR ++= sequences.iterator.map(s => new StringBuilder(s))
    abundR ++= this.abundances.map(ArrayBuffer() ++ _)

    for {
      (v, abund) <- values.iterator zip abundances
      if !findAndIncrement(v, seqR, abundR, n, abund)
    } {
      n = insertSequence(v, seqR, abundR, n, abund)
      sequencesUpdated = true
    }

    copy(seqR.filter(_ != null).map(_.toString).toArray,
      abundR.filter(_ != null).map(_.toArray).toArray, sequencesUpdated)
  }

  /**
   * Insert k-mer segments with corresponding abundances.
   * Each value is a segment with some number of overlapping k-mers. Each abundance item
   * applies to the whole of each such segments (all k-mers in a segment have the same abundance).
   *
   * This method is optimised for insertion of a larger number of distinct segments.
   */
  def insertBulkSegments(segmentsAbundances: Iterable[(String, Abundance)]): Self = {
    val insertAmt = segmentsAbundances.size

    val bufSize = insertAmt + sequences.size
    val seqR: ArrayBuffer[StringBuilder] = new ArrayBuffer(bufSize)
    val abundR: ArrayBuffer[ArrayBuffer[Abundance]] = new ArrayBuffer(bufSize)

    var n = sequences.size
    seqR ++= sequences.map(x => new StringBuilder(x))
    abundR ++= this.abundances.map(ArrayBuffer() ++ _)
    if (bufSize > helperMapThreshold) {
      initHelperMaps(seqR)
    }

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

    copy(seqR.filter(_ != null).map(_.toString).toArray,
      abundR.filter(_ != null).map(_.toArray).toArray, true)
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

  //If helperMap is set, then prefixHelper and suffixHelper are not null.

  //Maps the prefix of each sequence to the offset of that sequence.
  @transient
  var prefixHelper: MMap[String, Int] = MMap.empty

  //Maps the suffix of each sequence to the position of that sequence.
  @transient
  var suffixHelper: MMap[String, Int] = MMap.empty

  private def initHelperMaps(sequences: Iterable[StringBuilder]) {
    val r = MMap[NTSeq, (Int, Int)]()
    for {
      (seq, seqIdx) <- sequences.map(s => Read.kmers(s.toString, k)).zipWithIndex
      (kmer, kmerIdx) <- seq.zipWithIndex
    } {
      r += (kmer -> (seqIdx, kmerIdx))
    }
    helperMap = Some(r)

    prefixHelper = MMap[String, Int]()
    suffixHelper = MMap[String, Int]()
    for ((seq, i) <- sequences.zipWithIndex) {
      prefixHelper += (DNAHelpers.kmerPrefix(seq, k) -> i)
      suffixHelper += (DNAHelpers.kmerSuffix(seq, k) -> i)
    }
  }

  private def removeHelperSuffix(seq: StringBuilder) {
    helperMap match {
      case Some(m) =>
        suffixHelper -= DNAHelpers.kmerSuffix(seq, k)
      case _ =>
    }
  }

  private def removeHelperPrefix(seq: StringBuilder) {
    helperMap match {
      case Some(m) =>
        prefixHelper -= DNAHelpers.kmerPrefix(seq, k)
      case _ =>
    }
  }

  private def updateHelperMaps(newSeq: StringBuilder, i: Int) {
    updateHelperMaps(newSeq.toString, i)
  }

  private def updateHelperMaps(newSeq: NTSeq, i: Int) {
    helperMap match {
      case Some(m) =>
        for {
          (kmer, j) <- Read.kmers(newSeq, k).zipWithIndex
        } {
          m += (kmer -> (i, j))
        }
        prefixHelper += (DNAHelpers.kmerPrefix(newSeq, k) -> i)
        suffixHelper += (DNAHelpers.kmerSuffix(newSeq, k) -> i)
      case _ =>
    }
  }
}

object SimpleCountingBucket {
  def empty(k: Int) = new SimpleCountingBucket(Array(), Array(), k)
}

case class SimpleCountingBucket(override val sequences: Array[String],
  override val abundances: Array[Array[Abundance]],
  override val k: Int) extends CountingSeqBucket[SimpleCountingBucket](sequences, abundances, k)  {
  def copy(sequences: Array[String], abundances: Array[Array[Abundance]],
           sequencesUpdated: Boolean) =
        new SimpleCountingBucket(sequences, abundances, k)
}
