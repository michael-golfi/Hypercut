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

case class HashSegment(hash: Array[Byte], segment: String)
case class CountedHashSegment(hash: Array[Byte], segment: String, count: Long)

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import dbpart.bucket.CountingSeqBucket._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  type Kmer = String
  type Segment = String

  //For graph partitioning, it may help if this number is a square
  val NUM_PARTITIONS = 400

  spark.sqlContext.udf.register(
    "sharedOverlaps",
    (prior: Seq[String], post: Seq[String], k: Int) => PathMergingBucket.sharedOverlaps(prior, post, k))

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
  type ProcessedRead = (Array[HashSegment])

  /**
   * Hash and split a set of reads into marker sets and segments.
   */
  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      val hashesSegments = ext.splitRead(r, buckets)
      hashesSegments.map(x => HashSegment(x._1.compact.data, x._2)).toArray
    })
  }

  /**
   * Simplified version of bucketGraph that only builds buckets
   * (thus, counting k-mers), not collecting edges.
   */
  def bucketsOnly(reads: Dataset[String], ext: MarkerSetExtractor) = {
    val segments = splitReads(reads, ext)
    processedToBuckets(segments, ext)
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
                            reduce: Boolean = false): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val segments = reads.withColumnRenamed(reads.columns(0), "data").
      select(explode($"data").as("inner")).select("inner.*").
      as[HashSegment]

    val countedSegments =
      segments.groupBy($"hash", $"segment").count.
        as[CountedHashSegment]

    val byBucket = countedSegments.groupByKey(_.hash)

    byBucket.mapGroups {
      case (key, segmentsCounts) => {
        val empty = SimpleCountingBucket.empty(ext.k)
        val bkt = empty.insertBulkSegments(segmentsCounts.map(x => (x.segment, clipAbundance(x.count))).toList)
        (key, bkt)
      }
    }
  }

  /**
   * Construct a graph where the buckets are vertices, and edges are detected from
   * adjacent segments.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: Dataset[ProcessedRead], ext: MarkerSetExtractor,
                  writeLocation: Option[String] = None): GraphFrame = {
    val edges = reads.flatMap(r => MarkerSetExtractor.collectTransitions(r.map(_.hash).toList)).distinct()
    val verts = processedToBuckets(reads, ext)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
      reads.unpersist()
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
  def loadBucketGraph(readLocation: String, limit: Option[Int] = None): GraphFrame = {
    val (verts, edges) = loadBucketsEdges(readLocation)
    limit match {
      case Some(l) => toGraphFrame(verts.limit(l), edges)
      case _       => toGraphFrame(verts, edges)
    }
  }

  /**
   * By joining up buckets with sequences through incoming and outgoing edges,
   * construct PathMergingBuckets that are aware of their incoming and outgoing
   * openings.
   */
  def asMergingBuckets(readLocation: String, k: Int, limit: Option[Int] = None) = {
    val graph = loadBucketGraph(readLocation, limit)
    val msgCol = AggregateMessages.MSG_COL_NAME
    val aggCol = s"collect_set($msgCol)"

    val inOpen = graph.aggregateMessages.
      sendToDst("explode(sharedOverlaps(src.bucket.sequences, dst.bucket.sequences, src.bucket.k))").
      agg(aggCol).
      withColumnRenamed(aggCol, "inOpen")

    val outOpen = graph.aggregateMessages.
      sendToSrc("explode(sharedOverlaps(src.bucket.sequences, dst.bucket.sequences, src.bucket.k))").
      agg(aggCol).
      withColumnRenamed(aggCol, "outOpen")

    val joined = graph.vertices.join(inOpen, Seq("id"), "leftouter").
      join(outOpen, Seq("id"), "leftouter").
      as[(Array[Byte], SimpleCountingBucket, Array[String], Array[String])]

    joined.map(x => (x._1, PathMergingBucket(x._2.sequences, k, x._3, x._4)))
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
  def graphFromReads(input: String, ext: MarkerSetExtractor,
                     outputLocation: Option[String] = None) = {
    val reads = getReads(input)
    val split = splitReads(reads, ext)
    val r = bucketGraph(split, ext, outputLocation)
    split.unpersist
    r
  }
}

object InnerRoutines {
  def safeFlatten(ss: Array[Array[String]]) = {
    Option(ss) match {
      case Some(ss) => ss.iterator.filter(_ != null).flatten.toList
      case _        => List()
    }
  }
}
