package dbpart.spark

import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.google.common.hash.Hashing

import dbpart._
import dbpart.bucket._
import dbpart.hash._
import miniasm.genome.util.DNAHelpers
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import java.io.PrintStream
import java.io.File

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import dbpart.bucket.CountingSeqBucket._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql._
  import InnerRoutines._

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String): Dataset[String] = {
    val reads = sc.textFile(fileSpec).toDS
    reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
  }

  def countFeatures(reads: Dataset[String], space: MarkerSpace) = {
    reads.map(r => {
      val c = new FeatureCounter
      val s = new FeatureScanner(space)
      s.scanRead(c, r)
      c
    }).reduce(_ + _)
  }

  def hashReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, String)] = {
    reads.flatMap(r => ext.compactMarkers(r))
  }

  /*
   * The marker sets in a read, in sequence (in md5-hashed form),
   * and the corresponding segments that were discovered, in sequence.
   */
  type ProcessedRead = (Array[(Long, String)])

  /**
   * Process a set of reads, generating an intermediate dataset that contains both read segments
   * and edges between buckets.
   */
  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      val hashesSegments = ext.splitRead(r, buckets)
      hashesSegments.map(x => (longId(x._1.compact), x._2)).toArray
    })
  }

  def countKmers(reads: Dataset[String], ext: MarkerSetExtractor) = {
    val minCoverage = None
    val segments = splitReads(reads, ext)
    processedToBuckets(segments, ext, minCoverage)
  }

  def processedToBuckets[A](reads: Dataset[ProcessedRead], ext: MarkerSetExtractor,
                       minCoverage: Option[Coverage]): Dataset[(Long, SimpleCountingBucket)] = {
    val segments = reads.flatMap(x => x)
    val countedSegments =
      segments.groupByKey(x => x).count

    val byBucket = countedSegments.groupByKey({ case ((key, _), _) => key }).
      mapValues({ case ((_, read), count) => (read, count) })

    //Note: reduce, or an aggregator, may work better than mapGroups here.
    //mapGroups may cause high memory usage.
    val buckets = byBucket.mapGroups(
      {
        case (key, segmentsCounts) => {
          val empty = SimpleCountingBucket.empty(ext.k)
          val bkt = empty.insertBulkSegments(segmentsCounts.map(x => (x._1, clipCov(x._2))).toList).
            atMinCoverage(minCoverage)
          (key, bkt)
        }
      })
    buckets
  }

  /**
   * Compute buckets and edges between them.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: Dataset[ProcessedRead], ext: MarkerSetExtractor, minCoverage: Option[Coverage],
                  writeLocation: Option[String] = None) {
    val edges = reads.flatMap(r => MarkerSetExtractor.collectTransitions(r.map(_._1).toList)).distinct()
    val verts = processedToBuckets(reads, ext, minCoverage)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
      reads.unpersist()
    }
  }

  def writeBuckets(bkts: Dataset[(Long, SimpleCountingBucket)], writeLocation: String) {
    bkts.write.mode(SaveMode.Overwrite).parquet(s"${writeLocation}_buckets")
  }

  def loadBuckets(readLocation: String) =
     spark.read.parquet(s"${readLocation}_buckets").as[(Long, SimpleCountingBucket)]

  def loadBucketsEdges(readLocation: String) = {
    val edges = spark.read.parquet(s"${readLocation}_edges").as[(Long, Long)]
    val bkts = loadBuckets(readLocation)
    (bkts, edges)
  }

  def bucketStats(bkts: Dataset[(Long, SimpleCountingBucket)],
                  edges: Option[Dataset[(Long, Long)]]) {
    def smi(ds: Dataset[Int]) = ds.map(_.toLong).reduce(_ + _)
    def sms(ds: Dataset[Short]) = ds.map(_.toLong).reduce(_ + _)

    val fileStream = new PrintStream("bucketStats.txt")

    try {
      Console.withOut(fileStream) {
        val sequenceCount = bkts.map(_._2.sequences.size).cache
        val kmerCount = bkts.map(_._2.numKmers).cache
        val coverage = bkts.flatMap(_._2.coverages.toSeq.flatten).cache
        println("Sequence count in buckets: sum " + smi(sequenceCount))
        sequenceCount.describe().show()
        sequenceCount.unpersist
        println("k-mer count in buckets")
        //k-mer count sum can be seen in "count" from coverage.describe
        kmerCount.describe().show()
        kmerCount.unpersist
        println("k-mer coverage: sum " + sms(coverage))
        coverage.describe().show()
        coverage.unpersist

        for (e <- edges) {
          val outDeg = e.groupByKey(_._1).count()
          println("Outdegree: ")
          outDeg.describe().show()
          println("Indegree: ")
          val inDeg = e.groupByKey(_._2).count()
          inDeg.describe().show()
        }
      }
    } finally {
      fileStream.close()
    }
  }

  def buildBuckets(input: String, ext: MarkerSetExtractor, minCoverage: Option[Coverage],
                   outputLocation: Option[String] = None) = {
    val reads = getReads(input)
    val split = splitReads(reads, ext)
    val r = bucketGraph(split, ext, minCoverage, outputLocation)
    split.unpersist
    r
  }
}

object InnerRoutines {
  def longId(data: Array[Byte]): Long =
    Hashing.md5.hashBytes(data).asLong

  def longId(data: String): Long =
    Hashing.md5.hashString(data).asLong

  def longId(n: CompactNode): Long = longId(n.data)
}

object BuildBuckets extends SparkTool("Hypercut-BuildBuckets") {
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val k = args(2).toInt
//    val minCoverage = Some(args(3).toShort)
    val minCoverage = None
    val ext = MarkerSetExtractor.fromSpace("mixedTest", 5, k)
    val bkts = routines.buildBuckets(input, ext, minCoverage, Some(output))
  }
}

object BucketStats extends SparkTool("Hypercut-BucketStats") {
  def main(args: Array[String]) {
    val input = args(0)
    val (verts, edges) = if ((new File(s"${input}_edges")).exists) {
      routines.loadBucketsEdges(input) match {
        case (b,e) => (b, Some(e))
      }
    } else {
      (routines.loadBuckets(input), None)
    }
    routines.bucketStats(verts, edges)
  }
}

object CountKmers extends SparkTool("Hypercut-CountKmers") {
    def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val k = args(2).toInt
    val ext = MarkerSetExtractor.fromSpace("mixedTest", 5, k)
    val reads = routines.getReads(input)
    val bkts = routines.countKmers(reads, ext)
    routines.writeBuckets(bkts, output)
  }
}