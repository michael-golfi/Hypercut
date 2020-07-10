package hypercut.spark

import java.io.{File, PrintStream}

import hypercut.hash.{FeatureScanner, _}
import hypercut.bucket.BucketStats
import hypercut.graph.Contig
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer._
import miniasm.genome.util.DNAHelpers
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

final case class HashSegment(hash: BucketId, segment: ZeroBPBuffer)

final case class CountedHashSegment(hash: BucketId, segment: ZeroBPBuffer, count: Long)

/**
 * Core routines for executing Hypercut from Apache Spark.
 */
class Routines(val spark: SparkSession) {

  import SerialRoutines._

  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  def getReadsFromFiles(fileSpec: String, withRC: Boolean, k:Int,
                        sample: Option[Double] = None,
                        longSequence: Boolean = false): Dataset[String] =
    new HadoopReadFiles(spark, k).getReadsFromFiles(fileSpec, withRC,
      sample, longSequence)

  /**
   * Count motifs such as AC, AT, TTT in a set of reads.
   */
  def countFeatures(reads: Dataset[String], space: MotifSpace): FeatureCounter = {
    val brScanner = sc.broadcast(new FeatureScanner(space))
    //Repartition since for large data, too many partitions causes a lot of counters to be generated
    //and collected to the driver
    reads.repartition(100).mapPartitions(rs => {
      val s = brScanner.value
      val c = FeatureCounter(s.space)
      s.scanGroup(c, rs)
      Iterator(c)
    }).reduce(_ + _)
  }

  def createSampledSpace(input: Dataset[String], fraction: Double, template: MotifSpace,
                         persistLocation: Option[String] = None): MotifSpace = {
    val counter = countFeatures(input, template)
    counter.print(template, s"Discovered frequencies in fraction $fraction")

    //Optionally persist the counter for later reuse in a different run
    for (loc <- persistLocation) {
      val data = sc.parallelize(counter.motifsWithCounts(template), 100).toDS()
      data.write.mode(SaveMode.Overwrite).csv(s"${loc}_hash")
    }
    counter.toSpaceByFrequency(template, s"sampled$fraction")
  }


  /**
   * Restore persisted motif priorities. The template space must contain the same motifs,
   * but potentially in a different order.
   */
  def restoreSpace(location: String, template: MotifSpace): MotifSpace = {
    val raw = spark.read.csv(s"${location}_hash").map(x =>
      (x.getString(0), x.getString(1).toLong)).collect
    println(s"Restored previously saved hash parameters with ${raw.size} motifs")
    FeatureCounter.toSpaceByFrequency(template, raw, "restored")
  }

  /**
   * The motif sets in a read, paired with the corresponding segments that were discovered.
   */

  type ProcessedRead = (Array[HashSegment])


  def countedSegmentsByHash[H](segments: Dataset[HashSegment], spl: Broadcast[ReadSplitter[H]],
                               addReverseComplements: Boolean) = {
    val countedSegments = if (addReverseComplements) {
      val step1 =
        segments.groupBy($"hash", $"segment").count.
          as[CountedHashSegment].cache

      val reverseSegments = step1.flatMap(x => {
        val s = x.segment.toString
        val rc = DNAHelpers.reverseComplement(s)
        val revSegments = createHashSegments(rc, spl)
        revSegments.map(s => CountedHashSegment(s.hash, s.segment, x.count))
      })
      step1 union reverseSegments
    } else {
      //NB not caching in this case
      segments.groupBy($"hash", $"segment").count.
        as[CountedHashSegment]
    }

    val grouped = countedSegments.groupBy($"hash")
    grouped.agg(collect_list(struct($"segment", $"count"))).
      as[(BucketId, Array[(ZeroBPBuffer, Long)])]
  }

  def segmentsByHash[H](segments: Dataset[HashSegment],
                        addReverseComplements: Boolean) = {
    assert(!addReverseComplements) //not yet implemented

    val grouped = segments.groupBy($"hash")
    grouped.agg(collect_list($"segment")).
      as[(BucketId, Array[ZeroBPBuffer])]
  }


  def showStats(stats: Dataset[BucketStats]): Unit = {
    def sumLongs(ds: Dataset[Long]) = ds.reduce(_ + _)

    stats.cache
    println("Sequence count in buckets: sum " + sumLongs(stats.map(_.sequences)))
    println("Kmer count in buckets: sum " + sumLongs(stats.map(_.kmers)))
    println("k-mer abundance: sum " + sumLongs(stats.map(_.totalAbundance)))
    println("Bucket stats:")
    stats.describe().show()
    stats.unpersist
  }
}

/**
 * Serialization-safe routines.
 */
object SerialRoutines {
  def lengthFilter(minLength: Option[Int])(c: Contig) = minLength match {
    case Some(ml) => if (c.length >= ml) Some(c) else None
    case _ => Some(c)
  }

  def createHashSegments[H](r: String, spl: Broadcast[ReadSplitter[H]]): Iterator[HashSegment] = {
    val splitter = spl.value
    createHashSegments(r, splitter)
  }

  def createHashSegments[H](r: String, splitter: ReadSplitter[H]): Iterator[HashSegment] = {
    for {
      (h, s) <- splitter.split(r)
      r = HashSegment(splitter.compact(h), BPBuffer.wrap(s))
    } yield r
  }

  def createHashSegments[H, T](r: String, tag: T, splitter: ReadSplitter[H]): Iterator[(HashSegment, T)] = {
    for {
      (h, s) <- splitter.split(r)
      r = HashSegment(splitter.compact(h), BPBuffer.wrap(s))
    } yield (r, tag)
  }

  def createHashSegmentsFlat[H, T](r: String, tag: T, splitter: ReadSplitter[H]): Iterator[(BucketId, ZeroBPBuffer, T)] = {
    for {
      (h, s) <- splitter.split(r)
      r = (splitter.compact(h), BPBuffer.wrap(s), tag)
    } yield r
  }
}
