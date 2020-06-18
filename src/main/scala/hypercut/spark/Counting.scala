package hypercut.spark

import java.nio.ByteBuffer

import hypercut._
import hypercut.bucket.BucketStats
import hypercut.hash.{BucketId, ReadSplitter}
import hypercut.spark.SerialRoutines.createHashSegments
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Sorting

/**
 * Routines related to k-mer counting and statistics.
 * @param spark
 */
abstract class Counting[H](val spark: SparkSession, spl: ReadSplitter[H]) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)

  def toStatBuckets(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats]

  def countKmers[H](reads: Dataset[String]) = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val counts = segmentsToCounts(segments)
    countedWithStrings(counts)
  }

  def statisticsOnly[H](reads: Dataset[String], raw: Boolean): Unit = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val bkts = toStatBuckets(segments, raw)
    routines.showStats(bkts)
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Int], Long)]

  def writeCountedKmers[H](reads: Dataset[String], withKmers: Boolean, output: String) {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val counts = segmentsToCounts(segments)

    if (withKmers) {
      writeKmerCounts(countedWithStrings(counts), output)
    } else {
      writeKmerHistogram(counts.map(_._2), output)
    }
  }

  def countedWithStrings[H](counted: Dataset[(Array[Int], Long)]): Dataset[(String, Long)] = {
    val k = spl.k
    counted.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      val buffer = ByteBuffer.allocate(k / 4 + 4)
      val builder = new StringBuilder(k + 16)
      xs.map(x => (BPBuffer.intsToString(buffer, builder, x._1, 0.toShort, k.toShort), x._2))
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

final class GroupedCounting[H](s: SparkSession, spl: ReadSplitter[H],
                         addReverseComplements: Boolean) extends Counting(s, spl) {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import Counting._

  def countedToCounts(counted: Dataset[(BucketId, Array[(ZeroBPBuffer, Long)])], k: Int): Dataset[(Array[Int], Long)] = {
    counted.flatMap { case (hash, segmentsCounts) => {
      countsFromCountedSequences(segmentsCounts, k)
    } }
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Int], Long)] = {
    val bcSplit = this.bcSplit
    countedToCounts(routines.countedSegmentsByHash(segments, bcSplit, addReverseComplements),
      spl.k)
  }

  def toStatBuckets(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats] = {
    ???
  }
}

final class SimpleCounting[H](s: SparkSession, spl: ReadSplitter[H],
                        addReverseComplements: Boolean) extends Counting(s, spl) {

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import Counting._

  def uncountedToCounts(segments: Dataset[(BucketId, Array[ZeroBPBuffer])]): Dataset[(Array[Int], Long)] = {
    val k = spl.k
    segments.flatMap { case (hash, segments) => {
      countsFromSequences(segments, k)
    } }
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Int], Long)] = {
    uncountedToCounts(
      routines.segmentsByHash(segments, addReverseComplements))
  }

  def toStatBuckets(segments: Dataset[HashSegment], raw: Boolean): Dataset[BucketStats] = {
    val k = spl.k
    val byHash = routines.segmentsByHash(segments, addReverseComplements)
    if (raw) {
      byHash.map { case (hash, segments) => {
        //Simply count number of k-mers as a whole (including duplicates)
        //This algorithm should work even when the data is very skewed.
        val totalAbundance = segments.iterator.map(x => x.size.toLong - (k - 1)).sum
        BucketStats(segments.length, totalAbundance, 0)
      } }
    } else {
      byHash.map { case (hash, segments) => {
        val counted = countsFromSequences(segments, k)
        val (numDistinct, totalAbundance): (Long, Long) =
          counted.foldLeft((0L, 0L))((acc, item) => (acc._1 + 1, acc._2 + item._2))

        BucketStats(segments.length, totalAbundance, numDistinct)
      } }
    }
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