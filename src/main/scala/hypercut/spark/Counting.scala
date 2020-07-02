package hypercut.spark

import java.nio.ByteBuffer

import hypercut._
import hypercut.bucket.{BucketStats, ParentMap, SimpleBucket, TaxonBucket}
import hypercut.hash.{BucketId, ReadSplitter}
import hypercut.spark.SerialRoutines._
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

  def countKmers(reads: Dataset[String]) = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val counts = segmentsToCounts(segments)
    countedWithStrings(counts)
  }

  def statisticsOnly(reads: Dataset[String], raw: Boolean): Unit = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))

    val bkts = toStatBuckets(segments, raw)
    routines.showStats(bkts)
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Int], Long)]

  def segmentsToBuckets(segments: Dataset[HashSegment]): Dataset[SimpleBucket]

  def writeBuckets(reads: Dataset[String], output: String): Unit = {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val buckets = segmentsToBuckets(segments)
    buckets.write.mode(SaveMode.Overwrite).parquet(s"${output}_buckets")
  }

  def writeCountedKmers(reads: Dataset[String], withKmers: Boolean, histogram: Boolean, output: String) {
    val bcSplit = this.bcSplit
    val segments = reads.flatMap(r => createHashSegments(r, bcSplit))
    val counts = segmentsToCounts(segments)

    if (histogram) {
      writeCountsTable(countedToHistogram(counts), output)
    } else if (withKmers) {
      writeCountsTable(countedWithStrings(counts), output)
    } else {
      writeCountsTable(counts.map(_._2), output)
    }
  }

  def countedWithStrings(counted: Dataset[(Array[Int], Long)]): Dataset[(String, Long)] = {
    val k = spl.k
    counted.mapPartitions(xs => {
      //Reuse the byte buffer and string builder as much as possible
      //The strings generated here are a big source of memory pressure.
      val buffer = ByteBuffer.allocate(k / 4 + 4)
      val builder = new StringBuilder(k + 16)
      xs.map(x => (BPBuffer.intsToString(buffer, builder, x._1, 0.toShort, k.toShort), x._2))
    })
  }

  def countedToHistogram(counted: Dataset[(Array[Int], Long)]): Dataset[(Long, Long)] = {
    counted.map(_._2).groupBy("value").count().sort("value").as[(Long, Long)]
  }

  /**
   * Write k-mers and associated counts.
   * @param allKmers
   * @param writeLocation
   */
  def writeCountsTable[A](allKmers: Dataset[A], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_counts")
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

  def segmentsToBuckets(segments: Dataset[HashSegment]): Dataset[SimpleBucket] = {
    ???
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

  def uncountedToBuckets(segments: Dataset[(BucketId, Array[ZeroBPBuffer])]): Dataset[SimpleBucket] = {
    val k = spl.k
    segments.map { case (hash, segments) => {
      SimpleBucket.fromCountedSequences(hash, countsFromSequences(segments, k).toArray)
    } }
  }

  def segmentsToCounts(segments: Dataset[HashSegment]): Dataset[(Array[Int], Long)] = {
    uncountedToCounts(
      routines.segmentsByHash(segments, addReverseComplements))
  }

  def segmentsToBuckets(segments: Dataset[HashSegment]): Dataset[SimpleBucket] = {
    uncountedToBuckets(routines.segmentsByHash(segments, addReverseComplements))
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

final class TaxonBucketBuilder[H](val spark: SparkSession, spl: ReadSplitter[H],
                                  taxonomyNodes: String,
                                  addReverseComplements: Boolean = false) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._
  import Counting._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)
  val parentMap = getParentMap(taxonomyNodes)
  val bcParentMap = sc.broadcast(parentMap)


  /**
   * Read a taxon label file (TSV format)
   * This file is expected to be small.
   * @param file
   * @return
   */
  def getTaxonLabels(file: String): Dataset[(String, Int)] = {
    spark.read.option("sep", "\t").csv(file).
      map(x => (x.getString(0), x.getString(1).toInt))
  }

  /**
   * Read an ancestor map from an NCBI nodes.dmp style file.
   * This file is expected to be small.
   * @param file
   * @return
   */
  def getParentMap(file: String): ParentMap = {
    val raw = spark.read.option("sep", "|").csv(file).
      map(x => (x.getString(0).trim.toInt, x.getString(1).trim.toInt)).collect()
    val maxTaxon = raw.map(_._1).max
    val asMap = raw.toMap
    val asArray = (0 to maxTaxon).map(x => asMap.getOrElse(x, 1)).toArray
    asArray(1) = ParentMap.NONE //1 is root of tree
    new ParentMap(asArray)
  }

  def taggedToBuckets(segments: Dataset[(BucketId, Array[(ZeroBPBuffer, Int)])]): Dataset[TaxonBucket] = {
    val k = spl.k
    val bcPar = this.bcParentMap
    segments.map { case (hash, segments) => {
      TaxonBucket.fromTaggedSequences(hash, bcPar.value.taxonTaggedFromSequences(segments, k).toArray)
    } }
  }

  def segmentsToBuckets(segments: Dataset[(HashSegment, Int)]): Dataset[TaxonBucket] = {
    val grouped = segments.groupBy($"_1.hash")
    val byHash = grouped.agg(collect_list(struct($"_1.segment", $"_2"))).
      as[(BucketId, Array[(ZeroBPBuffer, Int)])]
    taggedToBuckets(byHash)
  }

  def writeBuckets(idsSequences: Dataset[(String, String)], labelFile: String, output: String): Unit = {
    val bcSplit = this.bcSplit
    val idSeqDF = idsSequences.toDF("id", "seq")
    val labels = getTaxonLabels(labelFile).toDF("id", "taxon")
    val joined = idSeqDF.join(broadcast(labels), idSeqDF("id") === labels("id")).
      select("seq", "taxon").as[(String, Int)]

    val segments = joined.flatMap(r => createHashSegments(r._1, r._2, bcSplit.value))
    val buckets = segmentsToBuckets(segments)
    buckets.write.mode(SaveMode.Overwrite).parquet(s"${output}_taxidx")
  }

  def classify(indexLocation: String, idsSequences: Dataset[(String, String)], k: Int, outputLocation: String): Unit = {
    val bcSplit = this.bcSplit
    val tbuckets = spark.read.parquet(s"${indexLocation}_taxidx").as[TaxonBucket]
    val idSeqDF = idsSequences.toDF("id", "seq").as[(String, String)]

    //Segments will be tagged with sequence ID
    val taggedSegments = idSeqDF.flatMap(r => createHashSegments(r._2, r._1, bcSplit.value))
    val grouped = taggedSegments.groupBy($"_1.hash")
    val byHash = grouped.agg(collect_list(struct($"_1.segment", $"_2"))).
      toDF("id", "data").
      as[(BucketId, Array[(ZeroBPBuffer, String)])]

    val indexWithInput = tbuckets.joinWith(byHash, tbuckets("id") === byHash("id"))
    val classified = indexWithInput.flatMap(x => x._1.classifyKmers(x._2._2, k))
    classified.write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${outputLocation}_classifiedPre")

    val bcPar = this.bcParentMap
    val tagLCA = classified.groupBy("_1").agg(collect_list($"_2")).
      as[(String, Array[Int])].map(x => (x._1, bcPar.value.classifySequence(x._2)))

    //TODO resolve_tree style algorithm
    tagLCA.write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${outputLocation}_classified")
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
  def tagOrdering[T]: Ordering[(Array[Int], T)] = new Ordering[(Array[Int], T)] {
    override def compare(x: (Array[Int], T), y: (Array[Int], T)): Int = {
      KmerOrdering.compare(x._1, y._1)
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
   * @param segments
   * @param k
   * @return
   */
  def countsFromSequences(segments: Iterable[BPBuffer], k: Int): Iterator[(Array[Int], Long)] = {
    val byKmer = segments.iterator.flatMap(s =>
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