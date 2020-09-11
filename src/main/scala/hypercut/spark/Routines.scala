package hypercut.spark

import hypercut.bucket.BucketStats
import hypercut.hash.{FeatureScanner, _}
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

    val r = reads.mapPartitions(rs => {
      val s = brScanner.value
      val c = FeatureCounter(s.space)
      s.scanGroup(c, rs)
      Iterator(c)
    })
    //If this number is too small, some CPUs might be idle
    //TODO: adjust/tune automatically
    r.coalesce(sc.defaultParallelism).reduce(_ + _)
  }

  def createSampledSpace(input: Dataset[String], fraction: Double, template: MotifSpace,
                         persistLocation: Option[String] = None,
                         motifFile: Option[String] = None): MotifSpace = {
    val counter = countFeatures(input, template)
    counter.print(template, s"Discovered frequencies in fraction $fraction")

    //Optionally persist the counter for later reuse in a different run
    for (loc <- persistLocation) {
      val data = sc.parallelize(counter.motifsWithCounts(template), 100).toDS()
      data.write.mode(SaveMode.Overwrite).csv(s"${loc}_hash")
    }

    val validMotifs = motifFile match {
      case Some(mf) =>
        val use = spark.read.csv(mf).collect().map(_.getString(0))
        println(s"${use.size} motifs will be used")
        use
      case _ =>
        template.byPriority
    }

    counter.toSpaceByFrequency(template, s"sampled$fraction", validMotifs)
  }


  /**
   * Restore persisted motif priorities. The template space must contain
   * (a subset of) the same motifs, but need not be in the same order.
   */
  def restoreSpace(location: String, template: MotifSpace): MotifSpace = {
    val raw = spark.read.csv(s"${location}_hash").map(x =>
      (x.getString(0), x.getString(1).toInt)).collect
    println(s"Restored previously saved hash parameters with ${raw.size} motifs")
    FeatureCounter.toSpaceByFrequency(template, raw, raw.map(_._1), "restored")
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
    def fmt(x: Any): String = {
      x match {
        case l: Long => l.toString
        case d: Double => "%.3f".format(d)
      }
    }

    val cols = Seq("kmers", "totalAbundance", "sequences")
    val aggCols = Array(sum("kmers"), sum("uniqueKmers"),
      sum("totalAbundance"), sum("sequences")) ++
      cols.flatMap(c => Seq(mean(c), min(c), max(c), stddev(c)))

    val statsAgg = stats.agg(count("sequences"), aggCols :_*).take(1)(0)
    val allValues = (0 until statsAgg.length).map(i => fmt(statsAgg.get(i)))

    val colfmt = "%-20s %s"
    println(colfmt.format("number of buckets", allValues(0)))
    println(colfmt.format("distinct k-mers", allValues(1)))
    println(colfmt.format("unique k-mers", allValues(2)))
    println(colfmt.format("total abundance", allValues(3)))
    println(colfmt.format("superkmer count", allValues(4)))
    println("Per bucket stats:")

    println(colfmt.format("", "Mean\tMin\tMax\tStd.dev"))
    for {
      (col: String, values: Seq[String]) <- (Seq("k-mers", "abundance", "superkmers").iterator zip
        allValues.drop(5).grouped(4))
    } {
      println(colfmt.format(col, values.mkString("\t")))
    }
  }
}

/**
 * Serialization-safe routines.
 */
object SerialRoutines {
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
}
