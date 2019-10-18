package dbpart.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.graphframes.GraphFrame
import dbpart.bucket.{BoundaryBucket, BoundaryBucketStats, BucketStats}
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.AggregateMessages
import dbpart.Contig
import dbpart.shortread.Read

import scala.collection.mutable.{Set => MSet}

/**
 * Iterative merge process that gradually collapses the graph by merging buckets with neighbors,
 * outputting sequence as we go along.
 */
class IterativeMerge(spark: SparkSession, showStats: Boolean = false,
  minLength: Option[Int], k: Int, output: String) {

  val AM = AggregateMessages
  val idSrc = AM.src("id")
  val idDst = AM.dst("id")
  val coreSrc = AM.src("core")
  val coreDst = AM.dst("core")
  val bndSrc = AM.src("boundary")
  val bndDst = AM.dst("boundary")
  val degSrc = AM.src("degree")
  val degDst = AM.dst("degree")

  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import SerialRoutines._
  import IterativeSerial._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  /*
   * Data being generated and consumed by each iteration.
   * Each vertex in the graph is a BoundaryBucket.
   */
  type IterationData = GraphFrame

  var iteration = 1
  var firstWrite = true
  def nextWriteMode = if (!firstWrite) "append" else "overwrite"
  val checkpointInterval = 2

  /**
   * Trick to get around the very large execution plans that arise
   * when running iterative algorithms on DataFrames.
   */
  def removeLineage(df: DataFrame) = {
    val r = AggregateMessages.getCachedDataFrame(df)
    if (iteration % checkpointInterval == 0) {
      r.checkpoint()
    } else {
      r
    }
  }

  def materialize[R](ds: Dataset[R]) = {
    ds.foreachPartition((r: Iterator[R]) => () )
    ds
  }

  def stats(show: Boolean, label: String, data: GraphFrame) {
    if (show) {
      println(label)
      val buckets = data.vertices.as[BoundaryBucket]
      buckets.map(_.coreStats).describe().show
      data.degrees.describe().show
    }
  }

  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   * Sets all sequences to boundary.
   */
  def initialize(graph: GraphFrame): IterationData = {
    val buckets =
      graph.vertices.selectExpr("id as bucketId",
        "struct(monotonically_increasing_id() as id, array() as core, bucket.sequences as boundary, bucket.k as k)").
          as[(Array[Byte], BoundaryBucket)].
          toDF("bucketId", "bucket")

    val edgeTranslation = buckets.selectExpr("bucketId", "bucket.id").cache
    val edges = translateAndNormalizeEdges(graph.edges, edgeTranslation).cache

    val idGraph = GraphFrame(buckets.selectExpr("bucket.*").cache,
      edges)
    materialize(edges)
    edgeTranslation.unpersist

    idGraph
  }


  /**
   * Output unitigs that are ready and then split each bucket into maximal disjoint parts.
   */
  def seizeUnitigsAndSplit(graph: GraphFrame): GraphFrame = {

    val k = this.k
    //Identify boundary and output/remove unitigs
    //Returns cores split into subparts based on remaining data
    val mergeData = graph.vertices.as[BoundaryBucket].map(b => {
      val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndSplit(b)
      (unitigs, parts)
    })

    val ml = minLength
    mergeData.selectExpr("explode(_1)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    val splitBuckets = mergeData.selectExpr("explode(_2)", "monotonically_increasing_id() as nid").cache
    val translate = splitBuckets.select("col.id", "nid")

    val nbkts = materialize(removeLineage(
      splitBuckets.selectExpr("nid as id", "col.core", "col.boundary", "col.k").toDF
    ))
    val nedges = materialize(removeLineage(
      translateAndNormalizeEdges(graph.edges, translate).cache
    ))
    splitBuckets.unpersist()

    GraphFrame(nbkts, nedges)
  }

  /**
   * Translate edges according to an ID translation table and
   * remove duplicates.
   * Also ensures that both A->B and B->A do not exist simultaneously for any A,B.
   */
  def translateAndNormalizeEdges(edges: DataFrame, translation: DataFrame) = {
      edges.toDF("src", "dst").
        join(translation.toDF("src", "newSrc"), Seq("src")).
        join(translation.toDF("dst", "newDst"), Seq("dst")).
        selectExpr("newSrc", "newDst").toDF("src", "dst").
        filter("!(src == dst)").
        select(when($"src" <= $"dst", $"src").otherwise($"dst").as("src"),
          when($"src" <= $"dst", $"dst").otherwise($"src").as("dst")
        ).distinct()
  }

  /**
   * After all iterations have finished, output any remaining data. Buckets are
   * treated as if they have no neighbors.
   */
  def finishBuckets(graph: GraphFrame) {
    val isolated = graph.vertices.as[BoundaryBucket]
    val ml = minLength
    val unitigs = isolated.flatMap(i => {
      val joint = BoundaryBucket(i.id, i.core ++ i.boundary, Array(), i.k)
      val unitigs = BoundaryBucket.seizeUnitigsAndSplit(joint)
      unitigs._1.flatMap(lengthFilter(ml)).map(formatUnitig)
    }).write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false
  }

  /**
   * Collect neighbors and promote boundary to core where possible,
   * returning the result as a graph with larger, unified buckets.
   * @param graph
   * @return
   */
  def collectBuckets(graph: GraphFrame): GraphFrame = {

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    val minIds = graph.aggregateMessages.
      sendToDst(idDst).sendToSrc(idSrc).
      sendToDst(idSrc).sendToSrc(idDst).
      agg(min(AM.msg).as("minId"))

    val aggVerts = graph.vertices.join(minIds, Seq("id")).join(graph.degrees, Seq("id")).cache

    val k = this.k

    //Note that data duplication is expected here. A node can send its data as a boundary but
    //still be used as a merge core by other nodes.
    val mergeBoundary = GraphFrame(aggVerts, graph.edges).
      aggregateMessages.
      sendToDst(when(idDst === AM.src("minId"),
        struct(struct(idSrc.as("id"), coreSrc.as("core"), bndSrc.as("boundary"), lit(k).as("k")), degSrc))).
      sendToSrc(when(idSrc === AM.dst("minId"),
        struct(struct(idDst.as("id"), coreDst.as("core"), bndDst.as("boundary"), lit(k).as("k")), degDst))).
      agg(collect_list(AM.msg).as("surrounding"))

    val withBuckets2 = mergeBoundary.
      join(aggVerts, Seq("id"), "right_outer").
      selectExpr("struct(id, core, boundary, k) as at", "(minId == id) as fullSurrounding", "surrounding")

    //TODO tune caching
    val r = materialize(withBuckets2.as[CollectedBuckets].map(_.unified).cache)
    val edgeTranslation = withBuckets2.as[CollectedBuckets].flatMap(_.edgeTranslation)
    val nextEdges = materialize(translateAndNormalizeEdges(graph.edges, edgeTranslation.toDF).cache)
    aggVerts.unpersist
    GraphFrame(r.toDF, nextEdges)
  }

  /**
   * Starting from a graph where edges represent potential bucket intersection,
   * construct a new one where only edges corresponding to actual intersection
   * remain.
   */
  def refineEdges(graph: GraphFrame): GraphFrame = {
    val boundaryOnly = graph.vertices.selectExpr("id as dst", "boundary as dstBoundary")
    //join edges with boundary data
    val bySrc = graph.edges.toDF("id", "dst").
      join(boundaryOnly, Seq("dst")).
      groupBy("id").
      agg(collect_list(struct("dst", "dstBoundary")).as("dstData")).
      as[(Long, Array[(Long, Array[String])])]

    val remEdges = graph.vertices.join(bySrc, Seq("id")).as[EdgeData].
      flatMap(d => {
        val finder = BoundaryBucket.overlapFinder(d.boundary, d.k)
        val remain = d.dstData.filter(x => finder.check(x._2))
        remain.map(r => (d.id, r._1))
      })

    val newEdges = materialize(remEdges.toDF("src", "dst").cache)
    graph.edges.unpersist
    GraphFrame(graph.vertices, newEdges)
  }

  /**
   * Run one merge iteration
   */
  def runIteration(graph: IterationData): IterationData = {
    var data = graph
    stats(showStats, "Start of iteration", data)
//    data.vertices.show()

    data = collectBuckets(data)
    stats(showStats, "Collected/promoted", data)
//    data.vertices.show()

    data = seizeUnitigsAndSplit(data)
    stats(showStats, "Seize/split", data)
//    data.vertices.show()

    data = refineEdges(data)
    stats(showStats, "Refined", data)

    data
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame) {
    val dtf = new SimpleDateFormat("HH:mm:ss")
    println(s"[${dtf.format(new Date)}] Initialize iterative merge")

    var data = refineEdges(initialize(graph))
    var n = data.edges.count

    while (n > 0) {
      println(s"[${dtf.format(new Date)}] Begin iteration $iteration ($n edges)")

      data = runIteration(data)
      n = data.edges.count
      iteration += 1
    }
    println(s"[${dtf.format(new Date)}] No edges left, finishing")
    finishBuckets(data)
    println(s"[${dtf.format(new Date)}] Iterative merge finished")
  }
}

final case class EdgeData(id: Long, boundary: Array[String],
  dstData: Array[(Long, Array[String])], k: Int)

/**
 * A bucket and some of its neighbors.
 * @param at The central bucket
 * @param fullSurrounding Whether all surrounding buckets could be collected
 * @param surrounding Surrounding buckets (that were collected) and their degrees in the graph
 */
final case class CollectedBuckets(at: BoundaryBucket, fullSurrounding: Boolean,
                                  surrounding: Array[(BoundaryBucket, Int)]) {

  @transient
  lazy val (lone, shared) = surrounding.partition(_._2 == 1)

  /**
   * Boundary is promoted to core if we have the full surroundings.
   * Lone neighbor boundary is always promoted to core since "at" was their only neighbor.
   */
  def unifiedCore =
    if (fullSurrounding)
      at.core ++ surrounding.flatMap(_._1.core) ++ lone.flatMap(_._1.boundary) ++ at.boundary
    else
      at.core ++ surrounding.flatMap(_._1.core) ++ lone.flatMap(_._1.boundary)

  /**
   * If we have the full surroundings, we only need to use the shared neighbors' boundary
   * as the new boundary.
   * Otherwise we keep the existing boundary and add to it.
   * @return
   */
  def unifiedBoundary =
    if (fullSurrounding)
      shared.flatMap(_._1.boundary)
    else
      shared.flatMap(_._1.boundary) ++ at.boundary

  def unified =
    BoundaryBucket(at.id, unifiedCore, unifiedBoundary, at.k)

  def edgeTranslation =
    Seq(at.id -> at.id) ++ surrounding.map(_._1.id -> at.id)
}

/**
 * Helper routines available for serializable functions.
 */
object IterativeSerial {
  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }
}