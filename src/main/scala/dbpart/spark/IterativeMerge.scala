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

  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext

  import IterativeSerial._
  import SerialRoutines._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  val AM = AggregateMessages
  val idSrc = AM.src("id")
  val idDst = AM.dst("id")
  val coreSrc = AM.src("core")
  val coreDst = AM.dst("core")
  val bndSrc = AM.src("boundary")
  val bndDst = AM.dst("boundary")

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
      materialize(edges))
    edgeTranslation.unpersist

    idGraph
  }


  /**
   * Output unitigs that are ready and then split the remainder of
   * each bucket into disjoint parts when possible.
   */
  def seizeUnitigsAndSplit(graph: GraphFrame): GraphFrame = {

    val k = this.k
    //Identify boundary and output/remove unitigs
    //Returns cores split into subparts based on remaining data
    val splitData = graph.vertices.as[BoundaryBucket].map(b => {
      val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndSplit(b)
      (b.id, unitigs, parts)
    })
    //TODO should splitData be cached?

    val ml = minLength
    splitData.selectExpr("explode(_2)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    val provisionalParts = materialize(splitData.selectExpr("_1 as id",
      "transform(_3, x -> x.boundary) as boundary",
      "transform(_3, x -> x.core) as core",
      "transform(_3, x -> monotonically_increasing_id()) as newIds").cache)

    val splitBoundary = provisionalParts.select("id", "boundary", "newIds").as[SplitBoundary]

    val builtEdges = edgesFromSplitBoundaries(splitBoundary, graph.edges.as[(Long, Long)])
    val builtBuckets = provisionalParts.selectExpr("explode(arrays_zip(newIds, core, boundary))")
    val nextBuckets = builtBuckets.selectExpr("col.newIds as id", "col.core as core", "col.boundary as boundary").
      withColumn("k", lit(k)).as[BoundaryBucket]

    val nb = materialize(removeLineage(nextBuckets.toDF))
    val ne = materialize(removeLineage(builtEdges.toDF("src", "dst", "intersection")))

    provisionalParts.unpersist
    graph.edges.unpersist
    graph.vertices.unpersist
    shiftBoundary(GraphFrame(nb, ne))
  }

  /**
   * Given split buckets and old edges, compute all the new edges that correspond to actual intersection
   * between the next-iteration split buckets.
   */
  def edgesFromSplitBoundaries(boundaries: Dataset[SplitBoundary], edges: Dataset[(Long, Long)]):
    Dataset[(Long, Long, Array[String])] = {
    val joint = boundaries.joinWith(edges, boundaries("id") === edges("src"))
    val srcByDst = joint.groupByKey(_._2._2)
    val byDst = boundaries.groupByKey(_.id)
    val k = this.k
    byDst.cogroup(srcByDst)((key, it1, it2) => {
      for {
        from <- it1
        to <- it2.map(_._1)
        edge <- from.edgesTo(to, k)
      } yield edge
    })
  }

  /**
   * Translate edges according to an ID translation table and
   * remove duplicates. The format of rows in the translation table is (old, new)
   * where old IDs will be renamed to new ones. This need not be one-to-one,
   * can be potentially many-to-many etc.
   * Also ensures that both A->B and B->A do not exist simultaneously for any A,B.
   * The double join is a performance concern and the size of the dataset can grow very large
   * if the translation map contains a lot of duplicates.
   */
  def translateAndNormalizeEdges(edges: DataFrame, translation: DataFrame) = {

    normalizeEdges(edges.toDF("src", "dst").
      join(translation.toDF("src", "newSrc"), Seq("src")).
      join(translation.toDF("dst", "newDst"), Seq("dst")).
      selectExpr("newSrc", "newDst"))
  }

  /**
   * Removes duplicates, orders edges in a standardized way, removes self edges.
   * @param edges
   * @return
   */
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
   * To do this with a minimum of redundancy, we identify the minimum ID of the neighborhood of each bucket,
   * and send its data there.
   *
   * @param graph
   * @return
   */
  def collectBuckets(graph: GraphFrame): GraphFrame = {
    val degSrc = AM.src("degree")
    val degDst = AM.dst("degree")

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    //This is the minimum of the IDs of all surrounding buckets plus the bucket's own ID.
    //Since edges are bidirectional, we send in both directions.
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
        struct(struct(idSrc.as("id"), coreSrc.as("core"), bndSrc.as("boundary"),
          lit(k).as("k")), degSrc))).
      sendToSrc(when(idSrc === AM.dst("minId"),
        struct(struct(idDst.as("id"), coreDst.as("core"), bndDst.as("boundary"),
          lit(k).as("k")), degDst))).
      agg(collect_list(AM.msg).as("surrounding"))

    val collected = aggVerts.
      join(mergeBoundary, Seq("id"), "left_outer").
      selectExpr("struct(id, core, boundary, k) as centre",
        "(if (isnotnull(degree), degree, 0)) as degree",
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
    graph.edges.unpersist
    graph.vertices.unpersist
    GraphFrame(r.toDF, nextEdges)
  }

  /**
   * Starting from a graph where edges represent potential bucket intersection,
   * construct a new one where only edges corresponding to actual intersection
   * remain.
   */
  def refineEdges(graph: GraphFrame): GraphFrame = {
    val boundaryOnly = graph.vertices.selectExpr("id as dst", "boundary as dstBoundary").
      as[(Long, Array[String])]

    val k = this.k
    val es = normalizeEdges(graph.edges).as[(Long, Long)]
    val bySrc = es.
      joinWith(boundaryOnly, es("dst") === boundaryOnly("dst"))

    val foundEdges = boundaryOnly.groupByKey(_._1).cogroup(
      bySrc.groupByKey(_._1._1))((key, it1, it2) => {
      for {
        from <- it1
        finder = BoundaryBucket.overlapFinder(from._2, k)
        to <- it2
        overlap = finder.find(to._2._2)
        if overlap.hasNext
        edge = (from._1, to._2._1, overlap.toArray)
      } yield edge
    })

    val newEdges = materialize(foundEdges.cache).toDF("src", "dst", "intersection")
//      normalized.join(withInt.select("src", "intersection"), Seq("src", "dst")).cache)

    graph.edges.unpersist
//    withInt.unpersist
    GraphFrame(graph.vertices, newEdges)
  }

  def shiftBoundary(graph: GraphFrame): GraphFrame = {
    val allIntersecting = graph.aggregateMessages.
      sendToDst(AM.edge("intersection")).
      sendToSrc(AM.edge("intersection")).
      agg(collect_list(AM.msg).as("intersection")).
      selectExpr("id", "(if (isnotnull(intersection), intersection, array(array()))) as intersection")

    val shiftedVert =
      graph.vertices.as[BoundaryBucket].joinWith(
        allIntersecting.as[(Long, Array[Array[String]])], graph.vertices("id") === allIntersecting("id"), "left_outer").
        map(row => {
          Option(row._2) match {
            case Some(int) => row._1.shiftBoundary(int._2.toSeq.flatten)
            case None => row._1.copy(core = row._1.core ++ row._1.boundary, boundary = Array())
          }
        })

    val newVerts = materialize(shiftedVert.cache)
    graph.vertices.unpersist
    GraphFrame(newVerts.toDF, graph.edges.select("src", "dst"))
  }

  /**
   * Run one merge iteration
   */
  def runIteration(graph: GraphFrame): GraphFrame = {
    var data = graph
    stats(showStats, "Start of iteration/split buckets", data)
    //    data.vertices.show()

    //At the start of each iteration, edges are refined (validated, guaranteed to correspond
    //to actual (k-1)-mer intersection between boundaries of two adjacent buckets.

    //Collect the neighborhood of selected buckets
    data = collectBuckets(data)
    stats(showStats, "Collected/promoted", data)
    //    data.vertices.show()

    //Extract and output unitigs from each bucket, splitting the remaining data if possible.
    //New edges will be correct by construction (no need to refine)
    data = seizeUnitigsAndSplit(data)
    data
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame) {
    val dtf = new SimpleDateFormat("HH:mm:ss")
    println(s"[${dtf.format(new Date)}] Initialize iterative merge")

    var data = shiftBoundary(refineEdges(initialize(graph)))
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

final case class SplitBoundary(id: Long, boundary: Array[Array[String]], newIds: Array[Long]) {
  def edgesTo(other: SplitBoundary, k: Int): Iterator[(Long, Long, Array[String])] =
    for {
      (from, fromNid) <- boundary.iterator zip newIds.iterator
      finder = BoundaryBucket.overlapFinder(from, k)
      (to, toNid) <- other.boundary.iterator zip other.newIds.iterator
      overlap = finder.find(to)
      if overlap.hasNext
      edge = (fromNid, toNid, overlap.toArray)
    } yield edge
}

/**
 * A bucket and some of its neighbors.
 *
 * @param centre      The central bucket
 * @param receiver    Whether this centre is a local minimum (receiver of neighbors).
 *                    If not it is duplicated elsewhere.
 * @param surrounding Surrounding buckets (that were collected) and their degrees in the graph
 */
final case class CollectedBuckets(centre: BoundaryBucket, degree: Int, receiver: Boolean,
                                  surrounding: Array[(BoundaryBucket, Int)]) {

  //Handle the possibility of a null value safely
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
    if (degree == 0)
      centre.core ++ centre.boundary
    else
    //TODO why can centre.boundary not be promoted to centre.core here in some cases?
        centre.core ++ safeSurrounding.flatMap(_._1.core) ++ lone.flatMap(_._1.boundary)
//      centre.core ++ deduplicate(safeSurrounding.flatMap(_._1.core) ++ lone.flatMap(_._1.boundary))

  def unifiedBoundary =
    if (degree == 0)
      Array[String]()
    else
      shared.flatMap(_._1.boundary) ++ centre.boundary

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