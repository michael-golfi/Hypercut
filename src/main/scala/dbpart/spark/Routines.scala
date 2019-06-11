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

  //For graph partitioning, it may help if this number is a square
  val NUM_PARTITIONS = 400

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

  /**
   * The marker sets in a read, in sequence (in md5-hashed form),
   * and the corresponding segments that were discovered, in sequence.
   */
  type ProcessedRead = (Array[(Array[Byte], String)])

  /**
   * Process a set of reads, generating an intermediate dataset that contains both read segments
   * and edges between buckets.
   */
  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      val hashesSegments = ext.splitRead(r, buckets)
      hashesSegments.map(x => (x._1.compact.data, x._2)).toArray
    })
  }

  /**
   * Simplified version that only builds buckets (thus counting k-mers), not collecting edges.
   */
  def countKmers(reads: Dataset[String], ext: MarkerSetExtractor) = {
    val minAbundance = None
    val segments = splitReads(reads, ext)
    processedToBuckets(segments, ext, minAbundance)
  }

  def countKmers(input: String, ext: MarkerSetExtractor, output: String) {
    val reads = getReads(input)
    val bkts = countKmers(reads, ext)
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
   * Construct a graph where the buckets are vertices.
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

    bucketGraph(verts, edges)
  }

  def bucketGraph(bkts: Dataset[(Array[Byte], SimpleCountingBucket)], edges: Dataset[(CompactEdge)]) = {
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
   * Build graph with edges only, setting all vertex attributes to 0.
   */
  def loadEdgeGraph(readLocation: String) = {
    //Note: probably better to repartition buckets and edges at an earlier stage, before
    //saving to parquet
    val (_, edges) = loadBucketsEdges(readLocation)
    val edgeDF = edges.toDF("src", "dst")
    GraphFrame.fromEdges(edgeDF)
  }

  /**
   * Build graph with edges and buckets (as vertices).
   */
  def loadBucketGraph(readLocation: String) = {
    val (verts, edges) = loadBucketsEdges(readLocation)
    bucketGraph(verts, edges)
  }

  def bucketStats(
    bkts:  Dataset[(Array[Byte], SimpleCountingBucket)],
    edges: Option[Dataset[CompactEdge]]) {
    def smi(ds: Dataset[Int]) = ds.map(_.toLong).reduce(_ + _)
    def sms(ds: Dataset[Short]) = ds.map(_.toLong).reduce(_ + _)

    val fileStream = new PrintStream("bucketStats.txt")

    try {
      Console.withOut(fileStream) {
        val sequenceCount = bkts.map(_._2.sequences.size).cache
        val kmerCount = bkts.map(_._2.numKmers).cache
        val abundance = bkts.flatMap(_._2.abundances.toSeq.flatten).cache
        println("Sequence count in buckets: sum " + smi(sequenceCount))
        sequenceCount.describe().show()
        sequenceCount.unpersist
        println("k-mer count in buckets")
        //k-mer count sum can be seen in "count" from abundance.describe
        kmerCount.describe().show()
        kmerCount.unpersist
        println("k-mer abundance: sum " + sms(abundance))
        abundance.describe().show()
        abundance.unpersist

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
  def buildBuckets(input: String, ext: MarkerSetExtractor, minAbundance: Option[Abundance],
                   outputLocation: Option[String] = None) = {
    val reads = getReads(input)
    val split = splitReads(reads, ext)
    val r = bucketGraph(split, ext, minAbundance, outputLocation)
    split.unpersist
    r
  }
}

object InnerRoutines {
//  def bfsProg(mod: Long)(id: VertexId, node: BucketNode, incoming: Long): BucketNode = {
//    if (incoming == -1) {
//      //Initial message
//      if (id % mod == 0) {
//        node.copy(partition = id / mod)
//      } else {
//        node
//      }
//    } else {
//      //partition should be -1
//      node.copy(partition = incoming)
//    }
//  }
//
//  def bfsMsg(triplet: EdgeTriplet[BucketNode, _]): Iterator[(VertexId, Long)] = {
//    val src = triplet.srcAttr
//    val dst = triplet.dstAttr
//    val it1 = if (src.partition != -1 && dst.partition == -1) Iterator((triplet.dstId, src.partition))
//      else Iterator.empty
//    val it2 = if (dst.partition != -1 && src.partition == -1) Iterator((triplet.srcId, dst.partition))
//      else Iterator.empty
//    it1 ++ it2
//  }
}
