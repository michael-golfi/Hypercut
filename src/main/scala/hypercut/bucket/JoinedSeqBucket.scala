package hypercut.bucket
import hypercut._
import hypercut.shortread.Read

import AbundanceBucket._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.IndexedSeq
import scala.collection.mutable.{Map => MMap}
import miniasm.genome.util.DNAHelpers
import hypercut.graph.PathGraphBuilder
import hypercut.graph.PathFinder
import hypercut.graph.KmerNode

import scala.annotation.tailrec

object JoinedSeqBucket {

  //At more than this many sequences, we build index maps to speed up the insertion process.
  //Without the maps we just perform a linear search.
  val helperMapThreshold = 16

  //If the number of sequences goes above this limit, we emit a warning.
  val warnBucketSize = 10000

  @volatile
  var mergeCount = 0
}

case class BucketStats(sequences: Long, totalAbundance: Long, kmers: Long)

/**
 * A bucket that counts the abundance of each k-mer and represents them as joined sequences.
 */
abstract class JoinedSeqBucket[+Self <: JoinedSeqBucket[Self]](val sequences: Array[String],
    val abundances: Array[Array[Abundance]], val k: Int) extends AbundanceBucket with Serializable {
  this: Self =>

  import JoinedSeqBucket._

  def kmers = kmersBySequence.flatten
  def kmersBySequence = sequences.toSeq.map(Read.kmers(_, k))
  def suffixes = kmers.map(DNAHelpers.kmerSuffix(_, k))
  def prefixes = kmers.map(DNAHelpers.kmerPrefix(_, k))

  def numKmers = sequences.map(_.length() - (k-1)).sum

  def stats = new BucketStats(sequences.length, abundances.flatten.map(_.toLong).sum, numKmers)

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
      case Some(abund) =>
        abundanceFilter(abund)
    }
  }

  def nonEmptyOption: Option[Self] = {
    if (numKmers == 0) None else Some(this)
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
   * Each input value is a segment with some number of overlapping k-mers. Each input abundance item
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
  def mergeBucket(other: JoinedSeqBucket[_]): Self = {
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
    r.sizeHint(sequences.size * 8)
    for {
      (seq, seqIdx) <- sequences.iterator.map(s => Read.kmers(s.toString, k)).zipWithIndex
      (kmer, kmerIdx) <- seq.zipWithIndex
    } {
      r += (kmer -> (seqIdx, kmerIdx))
    }
    helperMap = Some(r)

    prefixHelper = MMap[String, Int]()
    prefixHelper.sizeHint(sequences.size)
    suffixHelper = MMap[String, Int]()
    suffixHelper.sizeHint(sequences.size)
    for ((seq, i) <- sequences.iterator.zipWithIndex) {
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
  import JoinedSeqBucket._

  def empty(k: Int) = new SimpleCountingBucket(Array(), Array(), k)
}

//TODO remove the k parameter
case class SimpleCountingBucket(override val sequences: Array[String],
  override val abundances: Array[Array[Abundance]],
  override val k: Int) extends JoinedSeqBucket[SimpleCountingBucket](sequences, abundances, k)  {
  def copy(sequences: Array[String], abundances: Array[Array[Abundance]],
           sequencesUpdated: Boolean) =
        new SimpleCountingBucket(sequences, abundances, k)
}

object KmerBucket {

  @tailrec
  def collapseDuplicates(data: List[(String, Abundance)], acc: List[(String, Abundance)]): List[(String, Abundance)] = {
    data match {
      case k :: j :: ks =>
        if (k._1 == j._1) {
          collapseDuplicates(((k._1, clipAbundance(k._2 + j._2))) :: ks, acc)
        } else {
          collapseDuplicates(j :: ks, k :: acc)
        }
      case k :: ks => k :: acc
      case _ => acc
    }
  }

  /**
   * From a series of sequences (where k-mers may be repeated) and abundances,
   * produce an iterator with counted abundances where each k-mer appears only once.
   * @param segmentsAbundances
   * @param k
   * @return
   */
  def countsFromCountedSequences(segmentsAbundances: Iterable[(String, Long)], k: Int): Iterator[(String, Long)] = {
    val byKmer = segmentsAbundances.iterator.flatMap(s =>
      Read.kmers(s._1, k).map(km => (km, s._2))
    ).toList.sorted

    new Iterator[(String, Long)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.size

      def hasNext = i < len

      def next = {
        var lastKmer = remaining.head._1
        var count = 0L
        while (i < len && remaining.head._1 == lastKmer) {
          count += remaining.head._2
          i += 1
          remaining = remaining.tail
        }

        (lastKmer, count)
      }
    }
  }

  /**
   * From a series of sequences (where k-mers may be repeated),
   * produce an iterator with counted abundances where each k-mer appears only once.
   * @param segmentsAbundances
   * @param k
   * @return
   */
  def countsFromSequences(segmentsAbundances: Iterable[String], k: Int): Iterator[(String, Long)] = {
    val byKmer = segmentsAbundances.iterator.flatMap(s =>
      Read.kmers(s, k)
    ).toList.sorted

    new Iterator[(String, Long)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.size

      def hasNext = i < len

      def next = {
        var lastKmer = remaining.head
        var count = 0L
        while (i < len && remaining.head == lastKmer) {
          count += 1
          i += 1
          remaining = remaining.tail
        }

        (lastKmer, count)
      }
    }
  }
}
