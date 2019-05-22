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
  val NUM_PARTITIONS = 256

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
   * The two arrays will have equal length.
   */
  type ProcessedRead = (Array[Long], Array[String])

  /**
   * Process a set of reads, generating an intermediate dataset that contains both read segments
   * and edges between buckets.
   */
  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      val (hash, segment) = ext.splitRead(r, buckets).toList.unzip
      (hash.map(x => longId(x.compact)).toArray, segment.toArray)
    })
  }

  def hashToBuckets[A](reads: Dataset[ProcessedRead], ext: MarkerSetExtractor,
                       minCoverage: Option[Coverage]): Dataset[(Long, SimpleCountingBucket)] = {

    val readsOnly = reads.flatMap(_._2)
    val countedSegments =
      readsOnly.groupByKey(x => x).count
    //Restore hash by recomputing it
    val withHash = countedSegments.map(x =>
      (longId(ext.markerSetFor(x._1.substring(0, ext.k)).compact), x._1, x._2))

    val byBucket = withHash.groupByKey({ case (key, _, _) => key }).
      mapValues({ case (_, read, count) => (read, count) })
    val buckets = byBucket.mapGroups(
      {
        case (key, data) => {
          val segmentsCounts = data.toSeq
          val empty = SimpleCountingBucket.empty(ext.k)
          val bkt = empty.insertBulkSegments(segmentsCounts.map(_._1), segmentsCounts.map(c => clipCov(c._2))).
            atMinCoverage(minCoverage)
          (key, bkt)
        }
      })
    buckets
  }

  type BucketGraph = Graph[SimpleCountingBucket, Int]
  type PathGraph = Graph[PathNode, Int]
  type PathGraph2 = Graph[Int, PathNode]

  /**
   * Construct a GraphX graph where the buckets are vertices.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: Dataset[ProcessedRead], ext: MarkerSetExtractor, minCoverage: Option[Coverage],
                  writeLocation: Option[String] = None): BucketGraph = {
    reads.cache
    val edges = reads.flatMap(r => MarkerSetExtractor.collectTransitions(r._1.toList)).distinct()
    val verts = hashToBuckets(reads, ext, minCoverage)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      verts.write.mode(SaveMode.Overwrite).parquet(s"${output}_buckets")
      reads.unpersist()
    }

    val r = Graph.fromEdgeTuples(edges.rdd, 0).
      outerJoinVertices(verts.rdd)((vid, data, optBucket) => optBucket.get)
    r.cache
    r
  }

  def loadBucketsEdges(readLocation: String) = {
    val edges = spark.read.parquet(s"${readLocation}_edges").as[(Long, Long)]
    val bkts = spark.read.parquet(s"${readLocation}_buckets").as[(Long, SimpleCountingBucket)]
    (bkts, edges)
  }

  def loadBucketGraph(readLocation: String): BucketGraph = {
    val (verts, edges) = loadBucketsEdges(readLocation)
    val r = Graph.fromEdgeTuples(edges.rdd, 0).
      outerJoinVertices(verts.rdd)((vid, data, optBucket) => optBucket.get)
    r.cache
    r
  }

  def bucketStats(bkts: Dataset[(Long, SimpleCountingBucket)],
                  edges: Dataset[(Long, Long)]) {
    def smi(ds: Dataset[Int]) = ds.agg(sum(ds.columns(0))).collect()(0).get(0)
    def sms(ds: Dataset[Short]) = ds.agg(sum(ds.columns(0))).collect()(0).get(0)

    val fileStream = new PrintStream("bucketStats.txt")

    try {
      Console.withOut(fileStream) {
        val count = bkts.count()
        val sequenceCount = bkts.map(_._2.sequences.size).cache
        val kmerCount = bkts.map(_._2.numKmers).cache
        val coverage = bkts.flatMap(_._2.coverages.toSeq.flatten).cache
        println(s"Bucket count: $count")
        println("Sequence count: sum " + smi(sequenceCount))
        sequenceCount.describe().show()
        println("k-mer count: sum " + smi(kmerCount))
        kmerCount.describe().show()
        println("k-mer coverage: sum " + sms(coverage))
        coverage.describe().show()

        val outDeg = edges.groupByKey(_._1).count()
        println("Outdegree: ")
        outDeg.describe().show()
        println("Indegree: ")
        val inDeg = edges.groupByKey(_._2).count()
        inDeg.describe().show()
      }
    } finally {
      fileStream.close()
    }
  }

  def toPathGraph(graph: BucketGraph, k: Int): PathGraph = {
    val edges = graph.triplets.flatMap(tr => {
      val fromNodes = tr.srcAttr.sequencesWithCoverage.map(x => new PathNode(x._1, x._2))
      val toNodes = tr.dstAttr.sequencesWithCoverage.map(x => new PathNode(x._1, x._2))
      val edges = (for {
        from <- fromNodes; to <- toNodes
        if from.end(k) == to.begin(k)
        edge = (longId(from), longId(to))
      } yield edge)
      edges
    })

    val verts = graph.vertices.flatMap(v => v._2.sequencesWithCoverage.map(x =>
      (longId(x._1), new PathNode(x._1, x._2, seenId = longId(x._1)))))
    val r = Graph.fromEdgeTuples(edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).
      outerJoinVertices(verts)((vid, data, optNode) => optNode.get)
      .partitionBy(PartitionStrategy.EdgePartition2D, NUM_PARTITIONS)
      .cache
    r
  }

  /**
   * Traverse the graph in forward direction, merging unambiguous paths.
   */
  def mergePaths(graph: PathGraph, k: Int): RDD[String] = {

    val nodeIn = graph.inDegrees
    val nodeOut = graph.outDegrees
    var current = graph.joinVertices(nodeIn)((id, node, deg) => node.
      copy(inDeg = deg)).
      joinVertices(nodeOut)((id, node, deg) => node.copy(outDeg = deg)).
      removeSelfEdges()

    current.cache

    current.pregel(List[(VertexId, Int)](), Int.MaxValue,
      EdgeDirection.Out)(mergeProg(k), sendMsg(k), _ ++ _)

    current.vertices.groupBy(_._2.seenId).mapValues(joinPath(_, k)).values
  }

  def readsToPathGraph(input: String, ext: MarkerSetExtractor, minCoverage: Option[Coverage]) = {
    toPathGraph(buildBuckets(input, ext, minCoverage), ext.k)
  }

  def assemble(pg: PathGraph, k: Int, minCoverage: Option[Coverage], output: String) {
    val merged = mergePaths(pg, k)
    merged.cache

    val printer = new PathPrinter(output, false)
    for (contig <- merged.toLocalIterator) {
      printer.printSequence("hypercut-gx", contig, None)
    }

    printer.close()
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

  def longId(node: PathNode): Long =
    longId(node.seq)

  //Either start a path, or merge into one that we received a message from.
  def mergeProg(k: Int)(id: VertexId, node: PathNode, incoming: PathMsg): PathNode = {
    incoming match {
      case (seenId, seqPos) :: Nil =>
        if (seenId == node.seenId) {
          println(s"Loop detected at $id")
          node.copy(inLoop = true)
        } else {
          node.copy(seenId = seenId, seqPos = seqPos)
        }
      case Nil =>
        //Initial message
        if (node.startPoint) {
          node.copy(seqPos = 1)
        } else {
          node
        }
      case _ => node
    }
  }

  def sendMsg(k: Int)(triplet: EdgeTriplet[PathNode, Int]): Iterator[(VertexId, PathMsg)] = {
    val src = triplet.srcAttr
    val dst = triplet.dstAttr

    if (!dst.startPoint &&
      src.inPath &&
      (src.startPoint || !dst.stopPoint)) {
      Iterator((triplet.dstId, List(src.msg)))
    } else {
      Iterator.empty
    }
  }

  /**
   * Simple algorithm for concatenating a small number of unsorted path nodes.
   */
  def joinPath(nodes: Iterable[(VertexId, PathNode)], k: Int): String = {
    //TODO handling of coverage etc

    import scala.collection.mutable.{ Set => MSet }
    var rem = MSet() ++ nodes.map(_._2.seq)
    var build = rem.head
    rem -= build
    while (!rem.isEmpty) {
      val buildPre = DNAHelpers.kmerPrefix(build, k)
      val buildSuf = DNAHelpers.kmerSuffix(build, k)
      rem.find(_.endsWith(buildPre)) match {
        case Some(front) =>
          rem -= front
          build = DNAHelpers.withoutSuffix(front, k) + build
        case _ =>
      }

      rem.find(_.startsWith(buildSuf)) match {
        case Some(back) =>
          rem -= back
          build = build + DNAHelpers.withoutPrefix(back, k)
        case _ =>
      }
    }
    build
  }
}

object AssembleRaw extends SparkTool("Hypercut-AssembleRaw") {
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val k = args(2).toInt
    val minCoverage = Some(args(3).toShort)
    val ext = MarkerSetExtractor.fromSpace("mixedTest", 5, k)
    val pg = routines.readsToPathGraph(input, ext, minCoverage)
    routines.assemble(pg, k, minCoverage, output)
  }
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
    val (verts, edges) = routines.loadBucketsEdges(input)
    verts.cache
    edges.cache
    routines.bucketStats(verts, edges)
  }
}

object AssembleBuckets extends SparkTool("Hypercut-AssembleBuckets") {
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val k = args(2).toInt
    val pg = routines.toPathGraph(routines.loadBucketGraph(input), k)
    routines.assemble(pg, k, None, output)
  }
}