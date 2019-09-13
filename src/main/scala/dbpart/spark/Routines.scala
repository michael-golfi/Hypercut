package dbpart.spark

import java.io.File
import java.io.PrintStream

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages
import org.graphframes.lib.Pregel

import dbpart._
import dbpart.bucket._
import dbpart.hash._
import miniasm.genome.util.DNAHelpers

import dbpart.hash.FeatureScanner
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer._

import vegas._
import vegas.render.WindowRenderer._
import friedrich.util._

final case class HashSegment(hash: Array[Byte], segment: ZeroBPBuffer)
final case class CountedHashSegment(hash: Array[Byte], segment: ZeroBPBuffer, count: Long)

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  import SerialRoutines._

  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import dbpart.bucket.CountingSeqBucket._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  type Kmer = String
  type Segment = String

  //For graph partitioning, it may help if this number is a square
  val NUM_PARTITIONS = 400
  
  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String, withRC: Boolean, frac: Option[Double] = None): Dataset[String] = {
    //TODO: add boolean flag to include/not include RCs
    val reads = frac match {
      case Some(f) => sc.textFile(fileSpec).toDS.sample(f)
      case None => sc.textFile(fileSpec).toDS
    }
    
    if (withRC) {
      reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
    } else {
      reads
    }
  }

  /**
   * Count motifs such as AC, AT, TTT in a set of reads.
   */
  def countFeatures(reads: Dataset[String], space: MarkerSpace): FeatureCounter = {
    reads.map(r => {
      val c = new FeatureCounter
      val s = new FeatureScanner(space)
      s.scanRead(c, r)
      c
    }).reduce(_ + _)
  }

  def createSampledSpace(input: String, fraction: Double, space: MarkerSpace): MarkerSpace = {
    val in = getReads(input, true, Some(fraction))
    val counter = countFeatures(in, space)
    counter.print(s"Discovered frequencies in fraction $fraction")
    counter.toSpaceByFrequency(space.n)
  }

  /**
   * The marker sets in a read, paired with the corresponding segments that were discovered.
   */

  type ProcessedRead = (Array[HashSegment])

    /**
   * Hash and split a set of reads into edges
   */
  def splitReadsToEdges(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(Array[Byte], Array[Byte])] = {
    reads.flatMap(r => {
      val buckets = ext.markerSetsInRead(r)._2
      MarkerSetExtractor.collectTransitions(
        buckets.map(x => x._1.compact.data).toList
        )
    })
  }

  /**
   * Simplified version of bucketGraph that only builds buckets
   * (thus, counting k-mers), not collecting edges.
   */
  def bucketsOnly(reads: Dataset[String], ext: MarkerSetExtractor) = {
    val segments = reads.flatMap(r => createHashSegments(r, ext))
    processedToBuckets(segments, ext)
  }

  /**
   * Convenience function to compute buckets directly from an input specification
   * and write them to the output location.
   */
  def bucketsOnly(input: String, ext: MarkerSetExtractor, output: String) {
    val reads = getReads(input, false)
    val bkts = bucketsOnly(reads, ext)
    writeBuckets(bkts, output)
  }

  def processedToBuckets[A](segments: Dataset[HashSegment], ext: MarkerSetExtractor,
                            reduce: Boolean = false): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val countedSegments =
      segments.groupBy($"hash", $"segment").count.
        as[CountedHashSegment].cache
      
    val reverseSegments = countedSegments.flatMap(x => {
      val s = x.segment.toString
      val rc = DNAHelpers.reverseComplement(s)
      val revSegments = createHashSegments(rc, ext)
      revSegments.map(s => CountedHashSegment(s.hash, s.segment, x.count))
    } )
    val countedRev = reverseSegments.groupBy($"hash", $"segment").agg(expr("sum(count) as count")).
      as[CountedHashSegment]
    
    val grouped = countedSegments.union(countedRev).groupBy($"hash")
    
    val collected = grouped.agg(collect_list(struct($"segment", $"count"))).
      as[(Array[Byte], Array[(ZeroBPBuffer, Long)])]
    
    collected.map { case (hash, segmentsCounts) => {
        val empty = SimpleCountingBucket.empty(ext.k)
        val bkt = empty.insertBulkSegments(segmentsCounts.map(x =>
          (x._1.toString, clipAbundance(x._2))).toList)
        (hash, bkt)
      }
    }
  }

  def log10(x: Double) = Math.log(x) / Math.log(10)

  def plotBuckets(location: String, numBins: Int) {
    //Get the buckets
    val buckets = loadBuckets(location)
    val hist = new Histogram(buckets.map(_._2.numKmers).collect, numBins)
    val bins = (hist.bins.map(b => "%.1f".format(b)))
    val counts = hist.counts.map(x => log10(x))
    val points = bins zip counts
    //
    //      println(bins)
    hist.print("number of kmers")
    val plot = Vegas("Bucket Histogram", width=800, height=600).
      withData(
        points.map(p => Map("bins" -> p._1, "Kmers" -> p._2))).
        encodeX("bins", Quant).
        encodeY("Kmers", Quant).
        mark(Bar).show
  }

  /**
   * Construct a graph where the buckets are vertices, and edges are detected from
   * adjacent segments.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: String, ext: MarkerSetExtractor,
                  writeLocation: Option[String] = None): GraphFrame = {
    val edges = splitReadsToEdges(getReads(reads, false), ext).distinct
    val verts = bucketsOnly(getReads(reads, true), ext)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
    }

    toGraphFrame(verts, edges)
  }

  def toGraphFrame(
    bkts:  Dataset[(Array[Byte], SimpleCountingBucket)],
    edges: Dataset[(CompactEdge)]): GraphFrame = {
    val vertDF = bkts.toDF("id", "bucket")
    val edgeDF = edges.toDF("src", "dst")

//    GraphFrame(vertDF.repartition(NUM_PARTITIONS, vertDF("id")).cache,
//      edgeDF.repartition(NUM_PARTITIONS, edgeDF("src")).cache)
//
    GraphFrame(vertDF, edgeDF)
  }

  def writeBuckets(bkts: Dataset[(Array[Byte], SimpleCountingBucket)], writeLocation: String) {
    bkts.write.mode(SaveMode.Overwrite).parquet(s"${writeLocation}_buckets")
  }

  def loadBuckets(readLocation: String) =
    spark.read.parquet(s"${readLocation}_buckets").as[(Array[Byte], SimpleCountingBucket)]

  def loadBucketsEdges(readLocation: String) = {
    val edges = spark.read.parquet(s"${readLocation}_edges").as[CompactEdge]
    val bkts = loadBuckets(readLocation)
    (bkts, edges)
  }

  /**
   * Build graph with edges and buckets (as vertices).
   */
  def loadBucketGraph(readLocation: String, minAbundance: Option[Abundance] = None,
                      limit: Option[Int] = None): GraphFrame = {
    val (buckets, edges) = loadBucketsEdges(readLocation)
    val filterBuckets = minAbundance match {
      case Some(min) => buckets.map(x => (x._1, x._2.atMinAbundance(minAbundance)))
      case None => buckets
    }
    limit match {
      case Some(l) => toGraphFrame(filterBuckets.limit(l), edges)
      case _       => toGraphFrame(filterBuckets, edges)
    }
  }

  def bucketStats(
    bkts:  Dataset[(Array[Byte], SimpleCountingBucket)],
    edges: Option[Dataset[CompactEdge]],
    output: String) {

    def sml(ds: Dataset[Long]) = ds.reduce(_ + _)
    val fileStream = new PrintStream(s"${output}_bucketStats.txt")

    val stats = bkts.map(_._2.stats).cache

    try {
      Console.withOut(fileStream) {
        println("Sequence count in buckets: sum " + sml(stats.map(_.sequences.toLong)))
        println("Kmer count in buckets: sum " + sml(stats.map(_.kmers.toLong)))
        println("k-mer abundance: sum " + sml(stats.map(_.totalAbundance.toLong)))
        println("Bucket stats:")
        stats.describe().show()
        stats.unpersist

        for (e <- edges) {
          println(s"Total number of edges: ${e.count}")
          val outDeg = e.groupByKey(_._1).count().cache
          println("Outdegree: ")
          outDeg.describe().show()
          outDeg.unpersist
          println("Indegree: ")
          val inDeg = e.groupByKey(_._2).count().cache
          inDeg.describe().show()
          inDeg.unpersist
        }
      }
    } finally {
      fileStream.close()
    }
  }

  def bucketStats(input: String) {
    val (verts, edges) = if ((new File(s"${input}_edges")).exists) {
      loadBucketsEdges(input) match {
        case (b, e) => (b, Some(e))
      }
    } else {
      (loadBuckets(input), None)
    }
    bucketStats(verts, edges, input)
  }

  /**
   * Convenience function to build a GraphFrame directly from reads without
   * loading from saved data.
   * The generated data can optionally be saved in the specified location.
   */
  def graphFromReads(input: String, ext: MarkerSetExtractor,
                     outputLocation: Option[String] = None) = {

    val r = bucketGraph(input, ext, outputLocation)
    r
  }

  def showBuckets(buckets: Dataset[SimpleCountingBucket], n: Int, amount: Int) {
    val withSize = buckets.map(b => (b.sequences.size, b))
    val sorted = withSize.sort(desc("_1")).take(n)
    for { (size, bucket) <- sorted } {
      println(s"bucket size $size k-mers ${bucket.numKmers}")
      for { s <- bucket.sequences.take(amount) } {
        println(s"  $s")
      }
    }
  }
}

/**
 * Serialization-safe routines.
 */
object SerialRoutines {
  def removeN(segment: String, k: Int): Array[String] = {
    segment.split("N", -1).filter(s => s.length() >= k)
  }

  def safeFlatten(ss: Array[Array[String]]) = {
    Option(ss) match {
      case Some(ss) => ss.iterator.filter(_ != null).flatten.toList
      case _        => List()
    }
  }

  def lengthFilter(minLength: Option[Int])(c: Contig) = minLength match {
    case Some(ml) => if (c.length >= ml) Some(c) else None
    case _        => Some(c)
  }

  def createHashSegments(r: String, ext: MarkerSetExtractor) = {
    val buckets = ext.markerSetsInRead(r)._2
    val hashesSegments = ext.splitRead(r, buckets)
    hashesSegments.flatMap(x =>
      removeN(x._2, ext.k).map(s =>
        HashSegment(x._1.compact.data, BPBuffer.wrap(s))))
  }
}
