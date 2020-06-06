package hypercut.spark

import java.nio.ByteBuffer
import java.util

import hypercut._
import hypercut.bucket.BucketStats
import hypercut.hash.ReadSplitter
import hypercut.spark.SerialRoutines.createHashSegments
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.sql.SparkSession

import scala.util.Sorting

/**
 * Routines related to k-mer counting and statistics.
 * @param spark
 */
class Counting(spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import Counting._
  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  def countedToCounts(counted: Dataset[(Array[Byte], Array[(ZeroBPBuffer, Long)])], k: Int): Dataset[(Array[Int], Long)] =
    counted.flatMap { case (hash, segmentsCounts) => {
      countsFromCountedSequences(segmentsCounts, k)
    } }

  def uncountedToCounts(segments: Dataset[(Array[Byte], Array[ZeroBPBuffer])], k: Int): Dataset[(Array[Int], Long)] =
    segments.flatMap { case (hash, segments) => {
      countsFromSequences(segments, k)
    } }

  def uncountedToStatBuckets(segments: Dataset[(Array[Byte], Array[ZeroBPBuffer])], k: Int,
                             raw: Boolean): Dataset[BucketStats] = {
    if (raw) {
      segments.map { case (hash, segments) => {
        //Simply count number of k-mers as a whole (including duplicates)
        //This algorithm should work even when the data is very skewed.
        val totalAbundance = segments.iterator.map(x => x.size.toLong - (k - 1)).sum
        BucketStats(segments.length, totalAbundance, 0)
      } }
    } else {
      segments.map { case (hash, segments) => {
        val counted = countsFromSequences(segments, k)
        val (numDistinct, totalAbundance): (Long, Long) =
          counted.foldLeft((0L, 0L))((acc, item) => (acc._1 + 1, acc._2 + item._2))

        BucketStats(segments.length, totalAbundance, numDistinct)
      } }
    }
  }

  def countKmers[H](spl: ReadSplitter[H], reads: Dataset[String], addReverseComplements: Boolean, precount: Boolean) = {
    val segments = reads.flatMap(r => createHashSegments(r, spl))
    val counts = if (precount) {
      countedToCounts(
        routines.countedSegmentsByHash(segments, spl, addReverseComplements),
        spl.k)
    } else {
      uncountedToCounts(
        routines.segmentsByHash(segments, spl, addReverseComplements),
        spl.k)
    }
    countedWithStrings(spl, counts)
  }

  def statisticsOnly[H](spl: ReadSplitter[H], input: String, addReverseComplements: Boolean,
                        precount: Boolean, raw: Boolean): Unit = {
    val reads = routines.getReadsFromFasta(input, addReverseComplements)
    val segments = reads.flatMap(r => createHashSegments(r, spl))

    val bkts = if (precount) {
      ???
    } else {
      uncountedToStatBuckets(
        routines.segmentsByHash(segments, spl, addReverseComplements),
        spl.k, raw)
    }
    routines.showStats(bkts)
  }

  def writeCountedKmers[H](spl: ReadSplitter[H], input: String, addReverseComplements: Boolean,
                           precount: Boolean,
                           withKmers: Boolean,
                           output: String) {
    val reads = routines.getReadsFromFasta(input, addReverseComplements)
    val segments = reads.flatMap(r => createHashSegments(r, spl))

    val counts = if (precount) {
      countedToCounts(
        routines.countedSegmentsByHash(segments, spl, addReverseComplements),
        spl.k)
    } else {
      uncountedToCounts(
        routines.segmentsByHash(segments, spl, addReverseComplements),
        spl.k)
    }

    if (withKmers) {
      writeKmerCounts(countedWithStrings(spl, counts), output)
    } else {
      writeKmerHistogram(counts.map(_._2), output)
    }
  }

  def countedWithStrings[H](spl: ReadSplitter[H], counted: Dataset[(Array[Int], Long)]): Dataset[(String, Long)] = {
    counted.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      val buffer = ByteBuffer.allocate(spl.k / 4 + 4)
      val builder = new StringBuilder(spl.k + 16)
      xs.map(x => (BPBuffer.intsToString(buffer, builder, x._1, 0.toShort, spl.k.toShort), x._2))
    })
  }

  /**
   * Write k-mers and associated counts.
   * @param allKmers
   * @param writeLocation
   */
  def writeKmerCounts(allKmers: Dataset[(String, Long)], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_kmers")
  }

  /**
   * Write counts for each k-mer only, without the associated sequence.
   * @param histogram
   * @param writeLocation
   */
  def writeKmerHistogram(histogram: Dataset[Long], writeLocation: String): Unit = {
    histogram.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_hist")
  }

}

/**
 * Serialization-safe methods for counting
 */
object Counting {
  implicit object KmerOrdering extends Ordering[Array[Int]] {
    override def compare(x: Array[Int], y: Array[Int]): Int = {
      val l = x.length
      var i = 0
      while (i < l) {
        val a = x(i)
        val b = y(i)
        if (a < b) return -1
        else if (a > b) return 1
        i += 1
      }
      0
    }
  }

  /**
   * From a series of sequences (where k-mers may be repeated) and abundances,
   * produce an iterator with counted abundances where each k-mer appears only once.
   * @param segmentsAbundances
   * @param k
   * @return
   */
  def countsFromCountedSequences(segmentsAbundances: Iterable[(BPBuffer, Long)], k: Int): Iterator[(Array[Int], Long)] = {
    val byKmer = segmentsAbundances.iterator.flatMap(s =>
      s._1.kmersAsArrays(k.toShort).map(km => (km, s._2))
    ).toArray
    Sorting.quickSort(byKmer)

    new Iterator[(Array[Int], Long)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.length

      def hasNext = i < len

      def next = {
        val lastKmer = byKmer(i)._1
        var count = 0L
        while (i < len && java.util.Arrays.equals(byKmer(i)._1, lastKmer)) {
          count += byKmer(i)._2
          i += 1
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
  def countsFromSequences(segmentsAbundances: Iterable[BPBuffer], k: Int): Iterator[(Array[Int], Long)] = {
    val byKmer = segmentsAbundances.iterator.flatMap(s =>
      s.kmersAsArrays(k.toShort)
    ).toArray
    Sorting.quickSort(byKmer)

    new Iterator[(Array[Int], Long)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.length

      def hasNext = i < len

      def next = {
        val lastKmer = byKmer(i)
        var count = 0L
        while (i < len && java.util.Arrays.equals(byKmer(i), lastKmer)) {
          count += 1
          i += 1
        }

        (lastKmer, count)
      }
    }
  }
}