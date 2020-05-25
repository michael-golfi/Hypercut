package hypercut.spark

import hypercut._
import hypercut.bucket.AbundanceBucket.clipAbundance
import hypercut.bucket.{BucketStats, KmerBucket, SimpleCountingBucket}
import hypercut.hash.ReadSplitter
import hypercut.spark.SerialRoutines.createHashSegments
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * Routines related to k-mer counting and statistics.
 * @param spark
 */
class Counting(spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  def countedToKmerBuckets(counted: Dataset[(Array[Byte], Array[(ZeroBPBuffer, Long)])], k: Int) =
    counted.map { case (hash, segmentsCounts) => {
      val bkt = KmerBucket.fromCountedSequences(segmentsCounts.map(x =>
        (x._1.toString, clipAbundance(x._2))), k)
      (hash, bkt)
    } }

  def uncountedToKmerBuckets(segments: Dataset[(Array[Byte], Array[ZeroBPBuffer])], k: Int) =
    segments.map { case (hash, segments) => {
      val bkt = KmerBucket.fromCountedSequences(segments.map(x =>
        (x.toString, 1: Abundance)), k)
      (hash, bkt)
    } }

  def countedToCounts(counted: Dataset[(Array[Byte], Array[(ZeroBPBuffer, Long)])], k: Int): Dataset[(String, Abundance)] =
    counted.flatMap { case (hash, segmentsCounts) => {
      KmerBucket.countsFromCountedSequences(segmentsCounts.map(x =>
        (x._1.toString, clipAbundance(x._2))), k)
    } }

  def uncountedToCounts(segments: Dataset[(Array[Byte], Array[ZeroBPBuffer])], k: Int): Dataset[(String, Abundance)] =
    segments.flatMap { case (hash, segments) => {
      KmerBucket.countsFromSequences(segments.map(_.toString), k)
    } }

  def uncountedToStatBuckets(segments: Dataset[(Array[Byte], Array[ZeroBPBuffer])], k: Int): Dataset[BucketStats] =
    segments.map { case (hash, segments) => {
      val strings = segments.map(_.toString)
      val counted = KmerBucket.countsFromSequences(strings, k)

      val (numDistinct, totalAbundance): (Long, Long) =
        counted.foldLeft((0L, 0L))((a, b) => (a._1 + 1, a._2 + b._2))

      BucketStats(segments.length, totalAbundance, numDistinct)
    } }

  def statisticsOnly[H](spl: ReadSplitter[H], input: String, addReverseComplements: Boolean,
                        precount: Boolean): Unit = {
    val reads = routines.getReadsFromFasta(input, addReverseComplements)
    val segments = reads.flatMap(r => createHashSegments(r, spl))

    val bkts = if (precount) {
      ???
    } else {
      uncountedToStatBuckets(
        routines.segmentsByHash(segments, spl, addReverseComplements),
        spl.k)
    }
    routines.showStats(bkts, Console.out)
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
      writeKmerCounts(counts, output)
    } else {
      writeKmerHistogram(counts.map(_._2), output)
    }
  }

  /**
   * Write k-mers and associated counts.
   * @param allKmers
   * @param writeLocation
   */
  def writeKmerCounts(allKmers: Dataset[(String, Abundance)], writeLocation: String): Unit = {
    allKmers.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_kmers")
  }

  /**
   * Write counts for each k-mer only, without the associated sequence.
   * @param histogram
   * @param writeLocation
   */
  def writeKmerHistogram(histogram: Dataset[Abundance], writeLocation: String): Unit = {
    histogram.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${writeLocation}_hist")
  }


}
