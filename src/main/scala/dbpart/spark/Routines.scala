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

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import dbpart.bucket.CountingSeqBucket._

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String): Dataset[String] = {
    val reads = sc.textFile(fileSpec).toDF.map(_.getString(0))
    val withRev = reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
    withRev
  }

  def countFeatures(reads: Dataset[String], space: MarkerSpace) = {
    reads.map(r => {
      val c = new FeatureCounter
      val s = new FeatureScanner(space)
      s.scanRead(c, r)
      c
    }).reduce( _+_ )
  }

  def hashReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, String)] = {
    reads.flatMap(r => ext.compactMarkers(r))
  }

  type ProcessedRead = Array[(Array[Byte], String)]

  /**
   * Process a set of reads, generating an intermediate dataset that contains both read segments
   * and edges between buckets.
   */
  def splitReads(reads: Dataset[String],  ext: MarkerSetExtractor): Dataset[ProcessedRead] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      ext.splitRead(r, buckets).iterator.map(x => (x._1.compact.data, x._2)).toArray
    })
  }

  /**
   * Extract only segments from the dataset generated by the function above.
   */
  def segmentsFromSplit(reads: Dataset[ProcessedRead]) = reads.flatMap(x => x)

  /**
   * Extract only edges from the dataset generated by the function above.
   */
  def edgesFromSplit(reads: Dataset[ProcessedRead]) = reads.flatMap(s =>
    MarkerSetExtractor.collectTransitions(s.toList.map(_._1)))

  def hashToBuckets[A](reads: Dataset[Array[(Long, String)]], ext: MarkerSetExtractor,
      minCoverage: Option[Int]): Dataset[(Long, SimpleCountingBucket)] = {
    val split = reads.flatMap(x => x)
    val countedSegments =
      split.groupByKey(x => x).mapValues(_._2).count

    val byBucket = countedSegments.groupByKey( { case (key, count) => key._1 }).
      mapValues( { case (key, count) => (key._2, count) })
    val buckets = byBucket.mapGroups(
      { case (key, data) => {
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

  /**
   * Construct a GraphX graph where the buckets are vertices.
   */
  def bucketGraph(reads: Dataset[ProcessedRead], ext: MarkerSetExtractor, minCoverage: Option[Int]): BucketGraph = {
    val inner = new InnerRoutines
    val md5Hashed = reads.map(_.map(s => (inner.longId(s._1), s._2)))
    md5Hashed.cache
    val edges = md5Hashed.flatMap(r => MarkerSetExtractor.collectTransitions(r.toList.map(_._1))).distinct().rdd
    val verts = hashToBuckets(md5Hashed, ext, minCoverage)
    val r = Graph.fromEdgeTuples(edges, 0).outerJoinVertices(verts.rdd)((vid, data, optBucket) => optBucket.get)
    r.cache
    r
  }

  def toPathGraph(graph: BucketGraph, k: Int): PathGraph = {
    val inner = new InnerRoutines
    val edges = graph.triplets.flatMap(tr => {
      val fromNodes = tr.srcAttr.sequencesWithCoverage.map(x => new PathNode(x._1, x._2))
      val toNodes = tr.dstAttr.sequencesWithCoverage.map(x => new PathNode(x._1, x._2))
      val edges = (for {
        from <- fromNodes; to <- toNodes
        if from.end(k) == to.begin(k)
        edge = (inner.longId(from), inner.longId(to))
      }  yield edge)
      edges
    })

    val verts = graph.vertices.flatMap(v => v._2.sequencesWithCoverage.map(x =>
      (inner.longId(x._1), new PathNode(x._1, x._2, minId = inner.longId(x._1)))))
    val r = Graph.fromEdgeTuples(edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
        vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).
      outerJoinVertices(verts)((vid, data, optNode) => optNode.get)
    r.cache
    r
  }

  /**
   * Traverse the graph in forward direction, merging unambiguous paths.
   */
  def mergePaths(graph: PathGraph, k: Int) = {

    val nodeIn = graph.inDegrees
    val nodeOut = graph.outDegrees
    val prepared = graph.joinVertices(nodeIn)((id, node, branch) => node.
          copy(branchInOrLoop = branch > 1)).
      joinVertices(nodeOut)((id, node, deg) => node.copy(outDeg = deg)).
      removeSelfEdges()

    prepared.cache()
    val inner = new InnerRoutines

    prepared.pregel(List[(String, Double, VertexId)](), Int.MaxValue,
      EdgeDirection.Out)(inner.mergeProg(k), inner.sendMsg(k), _ ++ _)
  }

  def readsToPathGraph(reads: Dataset[String], ext: MarkerSetExtractor, minCoverage: Option[Int]) = {
    val split = splitReads(reads, ext)
    val bg = bucketGraph(split, ext, minCoverage)
    toPathGraph(bg, ext.k)
  }

  def assembleFiles(input: String, ext: MarkerSetExtractor, minCoverage: Option[Int], output: String) {
    val reads = getReads(input)
    val pg = readsToPathGraph(reads, ext, minCoverage)
    reads.unpersist()
    val merged = mergePaths(pg, ext.k)
    merged.cache

    val printer = new PathPrinter(output, false)
    val seqs = merged.vertices.flatMap(x => x._2.collectedSeq)

    for (contig <- seqs.toLocalIterator) {
      printer.printSequence("hypercut-gx", contig, None)
    }

    printer.close()
  }
}

final class InnerRoutines extends Serializable {
  def longId(data: Array[Byte]): Long =
    Hashing.md5.hashBytes(data).asLong

  def longId(data: String): Long =
    Hashing.md5.hashString(data).asLong

  def longId(node: PathNode): Long =
    longId(node.seq)

  type Msg = List[(String, Double, VertexId)]

  def mergeProg(k: Int)(id: VertexId, node: PathNode, incoming: Msg): PathNode = {
    if (node.stopPoint) {
      node.collect(incoming.map(_._1), k)
    } else {
      incoming match {
        case (seq, cov, minId) :: Nil =>
          if (minId == id) {
            println(s"Loop detected at $id")
            node.copy(branchInOrLoop = true)
          } else {
            node.copy(seq = seq, avgCoverage = cov).observeVertex(minId)
          }
        case Nil =>
          //Initial message
          node
        case _ => ???
      }
    }
  }

  def sendMsg(k: Int)(triplet: EdgeTriplet[PathNode, Int]): Iterator[(VertexId, Msg)] = {
    val src = triplet.srcAttr
    if (!src.stopPoint) {
      Iterator((triplet.dstId, List(src.msg)))
    } else {
      Iterator.empty
    }
  }

}

object BuildGraph {
  def conf: SparkConf = {
    val conf = new SparkConf
    conf.set("spark.kryo.classesToRegister", "dbpart.bucket.SimpleCountingBucket,dbpart.hash.CompactNode,dbpart.spark.PathNode")
    GraphXUtils.registerKryoClasses(conf)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Hypercut").
      master("spark://localhost:7077").config(conf).getOrCreate()
    val input = args(0)
    val routines = new Routines(spark)
    val output = args(1)
    val k = args(2).toInt
    val minCoverage = Some(args(3).toInt)
    val ext = MarkerSetExtractor.fromSpace("mixedTest", 5, k)
    routines.assembleFiles(input, ext, minCoverage, output)
  }
}
