package dbpart.spark

import org.apache.spark.SparkConf
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
import org.graphframes.GraphFrame
import org.graphframes.lib.Pregel

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

  type Kmer = String
  type Segment = String

  //For graph partitioning, it may help if this number is a square
  val NUM_PARTITIONS = 400

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String): Dataset[String] = {
    val reads = sc.textFile(fileSpec).toDS
    reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
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

  /**
   * Find MarkerSets (in the compacted version) in a set of reads,
   * returning them together with associated k-mers.
   */
  def hashReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, Kmer)] = {
    reads.flatMap(r => ext.compactMarkers(r))
  }

  /**
   * The marker sets in a read, paired with the corresponding segments that were discovered.
   */
  type ProcessedRead = (Array[(Array[Byte], Segment)])

  /**
   * Hash and split a set of reads into marker sets and segments.
   */
  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      val hashesSegments = ext.splitRead(r, buckets)
      hashesSegments.map(x => (x._1.compact.data, x._2)).toArray
    })
  }

  /**
   * Simplified version of bucketGraph that only builds buckets
   * (thus, counting k-mers), not collecting edges.
   */
  def bucketsOnly(reads: Dataset[String], ext: MarkerSetExtractor) = {
    val minAbundance = None
    val segments = splitReads(reads, ext)
    processedToBuckets(segments, ext, minAbundance)
  }

  /**
   * Convenience function to perform bucketsOnly directly from an input file.
   */
  def bucketsOnly(input: String, ext: MarkerSetExtractor, output: String) {
    val reads = getReads(input)
    val bkts = bucketsOnly(reads, ext)
    writeBuckets(bkts, output)
  }

  def processedToBuckets[A](reads: Dataset[ProcessedRead], ext: MarkerSetExtractor,
                       minAbundance: Option[Abundance]): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val segments = reads.flatMap(x => x)
    val countedSegments =
      segments.groupBy(segments.columns(0), segments.columns(1)).count.
      as[(Array[Byte], String, Long)]

    val byBucket = countedSegments.groupByKey({ case (key, _, _) => key }).
      mapValues({ case (_, read, count) => (read, count) })

    //Note: reduce, or an aggregator, may work better than mapGroups here.
    //mapGroups may cause high memory usage.
    val buckets = byBucket.mapGroups(
      {
        case (key, segmentsCounts) => {
          val empty = SimpleCountingBucket.empty(ext.k)
          val bkt = empty.insertBulkSegments(segmentsCounts.map(x => (x._1, clipAbundance(x._2))).toList).
            atMinAbundance(minAbundance)
          (key, bkt)
        }
      })
    buckets
  }

  /**
   * Construct a graph where the buckets are vertices, and edges are detected from
   * adjacent segments.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: Dataset[ProcessedRead], ext: MarkerSetExtractor, minAbundance: Option[Abundance],
                  writeLocation: Option[String] = None): GraphFrame = {
    val edges = reads.flatMap(r => MarkerSetExtractor.collectTransitions(r.map(_._1).toList)).distinct()
    val verts = processedToBuckets(reads, ext, minAbundance)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
      reads.unpersist()
    }

    toGraphFrame(verts, edges)
  }

  def toGraphFrame(bkts: Dataset[(Array[Byte], SimpleCountingBucket)],
                   edges: Dataset[(CompactEdge)]): GraphFrame = {
    val vertDF = bkts.toDF("id", "bucket")
    val edgeDF = edges.toDF("src", "dst")
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
   * Using pre-generated and saved data, build a GraphFrame with edges only (no buckets).
   */
  def loadEdgeGraph(readLocation: String): GraphFrame = {
    //Note: probably better to repartition buckets and edges at an earlier stage, before
    //saving to parquet
    val (_, edges) = loadBucketsEdges(readLocation)
    val edgeDF = edges.toDF("src", "dst")
    GraphFrame.fromEdges(edgeDF)
  }

  /**
   * Build graph with edges and buckets (as vertices).
   */
  def loadBucketGraph(readLocation: String): GraphFrame = {
    val (verts, edges) = loadBucketsEdges(readLocation)
    toGraphFrame(verts, edges)
  }

  /**
   * By joining up buckets with sequences through incoming and outgoing edges,
   * construct PathMergingBuckets that are aware of their incoming and outgoing
   * openings.
   */
  def asMergingBuckets(readLocation: String) = {
    val (verts, edges) = loadBucketsEdges(readLocation)

    val fedges = edges.filter(x => (x._1 != x._2))

    val joinSrc = fedges.joinWith(verts, fedges("_1") === verts("_1")).map(x =>
        (x._1._2, x._2._2)).groupByKey(_._1).mapValues(_._2.sequences).
      mapGroups((k, vs) => (k, vs.toSeq.flatten))

    val joinDst = fedges.joinWith(verts, fedges("_2") === verts("_1")).map(x =>
        (x._1._1, x._2._2)).groupByKey(_._1).mapValues(_._2.sequences).
      mapGroups((k, vs) => (k, vs.toSeq.flatten))

    val r = verts.join(joinSrc.withColumnRenamed("_2", "priorSeqs"), Seq("_1"), "outer").
      join(joinDst.withColumnRenamed("_2", "postSeqs"), Seq("_1"), "outer").
      as[(Array[Byte], SimpleCountingBucket, Array[String], Array[String])].
      map(x => (x._1, x._2.asPathMerging(x._3, x._4)))
    r.cache
    r
  }

  def bucketStats(
    bkts:  Dataset[(Array[Byte], SimpleCountingBucket)],
    edges: Option[Dataset[CompactEdge]]) {

    def sml(ds: Dataset[Long]) = ds.reduce(_ + _)
    val fileStream = new PrintStream("bucketStats.txt")

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
          println(s"Total number of edges: " + e.count())
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
    bucketStats(verts, edges)
  }

  /**
   * Partition buckets using a BFS search.
   * @param modulo Starting points for partitions (id % modulo == 0).
   * The number of partitions will be inversely proportional to this number.
   */
  def partitionBuckets(graph: GraphFrame)(modulo: Long = 10000): GraphFrame = {
//    val nodeIn = graph.inDegrees
//    val nodeOut = graph.outDegrees
//
//    val nVert = graph.vertices.join(nodeIn, "id").join(nodeOut, "id").

    val partitions = graph.pregel.
      withVertexColumn("partition", lit(null),
        coalesce(Pregel.msg, lit(null))).
        sendMsgToDst(Pregel.src("partition")).
        sendMsgToSrc(Pregel.dst("partition")).
        aggMsgs(first(Pregel.msg)).
        run

//    var current = graph.mapVertices { case(id, _) => new BucketNode() }
//      joinVertices(nodeIn)((id, node, deg) => node.copy(inDeg = deg)).
//      joinVertices(nodeOut)((id, node, deg) => node.copy(outDeg = deg))
//
//    val result = current.pregel(-1L, Int.MaxValue, EdgeDirection.Either)(bfsProg(modulo),
//        bfsMsg, (x, y) => x)
//
//    //Some nodes may not have been reached - will be left in partition "-1".
//
//    result
        ???
  }

//  def assemble(pg: PathGraph, k: Int, minAbundance: Option[Abundance], output: String) {
//    val merged = mergePaths(pg, k)
//    merged.cache
//
//    val printer = new PathPrinter(output, false)
//    for (contig <- merged.toLocalIterator) {
//      printer.printSequence("hypercut-gx", contig, None)
//    }
//
//    printer.close()
//  }
//

  /**
   * Convenience function to build a GraphFrame directly from reads without
   * loading from saved data.
   * The generated data can optionally be saved in the specified location.
   */
  def graphFromReads(input: String, ext: MarkerSetExtractor, minAbundance: Option[Abundance],
                   outputLocation: Option[String] = None) = {
    val reads = getReads(input)
    val split = splitReads(reads, ext)
    val r = bucketGraph(split, ext, minAbundance, outputLocation)
    split.unpersist
    r
  }
}

object InnerRoutines {

}
