package dbpart.spark

import java.text.SimpleDateFormat
import java.util.Date

import dbpart.Contig
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

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

  import IterativeSerial._
  import SerialRoutines._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

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
    ds.foreachPartition((r: Iterator[R]) => ())
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
  def initialize(graph: GraphFrame): GraphFrame = {
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
    val splitData = graph.vertices.as[BoundaryBucket].map(b => {
      val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndSplit(b)
      (unitigs, parts)
    })

    val ml = minLength
    splitData.selectExpr("explode(_1)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    val splitBuckets = splitData.selectExpr("explode(_2)", "monotonically_increasing_id() as nid").cache
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
    normalizeEdges(edges.toDF("src", "dst").
      join(translation.toDF("src", "newSrc"), Seq("src")).
      join(translation.toDF("dst", "newDst"), Seq("dst")).
      selectExpr("newSrc", "newDst"))
  }

  def normalizeEdges(edges: DataFrame) = {
    edges.toDF("src", "dst").
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
   *
   * @param graph
   * @return
   */
  def collectBuckets(graph: GraphFrame): GraphFrame = {

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    val minIds = graph.aggregateMessages.
      sendToDst(array_min(array(idDst, idSrc))).
      sendToSrc(array_min(array(idDst, idSrc))).
      agg(min(AM.msg).as("minId"))

    val aggVerts = graph.vertices.join(minIds, Seq("id"), "left_outer").
      join(graph.degrees, Seq("id"), "left_outer").cache

    val k = this.k

    val mergeBoundary = GraphFrame(aggVerts, graph.edges).
      aggregateMessages.
      sendToDst(when(idDst === AM.src("minId"),
        struct(struct(idSrc.as("id"), coreSrc.as("core"), bndSrc.as("boundary"), lit(k).as("k")), degSrc))).
      sendToSrc(when(idSrc === AM.dst("minId"),
        struct(struct(idDst.as("id"), coreDst.as("core"), bndDst.as("boundary"), lit(k).as("k")), degDst))).
      agg(collect_list(AM.msg).as("surrounding"))

    val collected = aggVerts.
      join(mergeBoundary, Seq("id"), "left_outer").
      selectExpr("struct(id, core, boundary, k) as centre",
        "(if (isnotnull(minId), (minId == id), true)) as receiver",
        "surrounding").as[CollectedBuckets].cache

//    if (showStats) {
//      collected.show
//    }

    val r = materialize(collected.flatMap(_.unified).cache)
    val edgeTranslation = collected.flatMap(_.edgeTranslation)
    val nextEdges = materialize(translateAndNormalizeEdges(graph.edges, edgeTranslation.toDF).cache)
    aggVerts.unpersist
    collected.unpersist
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

    val newEdges = materialize(normalizeEdges(remEdges.toDF).cache)
    graph.edges.unpersist
    GraphFrame(graph.vertices, newEdges)
  }

  /**
   * Run one merge iteration
   */
  def runIteration(graph: GraphFrame): GraphFrame = {
    var data = graph
    stats(showStats, "Start of iteration/refined edges", data)
    //    data.vertices.show()

    data = collectBuckets(data)
    stats(showStats, "Collected/promoted", data)
    //    data.vertices.show()

    data = seizeUnitigsAndSplit(data)
    stats(showStats, "Seize/split", data)
    //    data.vertices.show()

    data = refineEdges(data)
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
 *
 * @param centre      The central bucket
 * @param receiver    Whether this centre is a local minimum (receiver of neighbors).
 *                    If not it is duplicated.
 * @param surrounding Surrounding buckets (that were collected) and their degrees in the graph
 */
final case class CollectedBuckets(centre: BoundaryBucket, receiver: Boolean,
                                  surrounding: Array[(BoundaryBucket, Int)]) {

  def safeSurrounding = Option(surrounding).getOrElse(Array())

  @transient
  lazy val (lone, shared) = safeSurrounding.partition(_._2 == 1)

  @transient
  lazy val centreData = centre.core ++ centre.boundary

  def deduplicate(data: Iterable[String]) = BoundaryBucket.removeKmers(centreData, data, centre.k)

  /**
   * Lone neighbor boundary is always promoted to core since "centre" was their only neighbor.
   */
  def unifiedCore =
    if (receiver && safeSurrounding.isEmpty)
      centre.core ++ centre.boundary
    else
      centre.core ++ deduplicate(safeSurrounding.flatMap(_._1.core) ++ lone.flatMap(_._1.boundary))

  def unifiedBoundary =
    if (receiver && safeSurrounding.isEmpty)
      Seq()
    else
      deduplicate(shared.flatMap(_._1.boundary)) ++ centre.boundary

  def unified: Iterable[BoundaryBucket] = {
    if (safeSurrounding.isEmpty && !receiver) {
      Seq()
    } else if (!receiver) {
      surrounding.map(_._1)
    } else {
      Seq(BoundaryBucket(unifiedBucketId, unifiedCore, unifiedBoundary.toArray, centre.k))
    }
  }

  def unifiedBucketId = if (safeSurrounding.isEmpty) centre.id else safeSurrounding.head._1.id
  
  def edgeTranslation: Seq[(Long, Long)] = {
    if (receiver) {
      safeSurrounding.toSeq.map(_._1.id -> unifiedBucketId) :+ (centre.id -> unifiedBucketId)
    } else {
      safeSurrounding.toSeq.map(x => (x._1.id -> x._1.id))
    }
  }

  override def toString = s"CB(${centre.id} $receiver ${safeSurrounding.size})"
}

/**
 * Helper routines available for serializable functions.
 */
object IterativeSerial {
  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }
}