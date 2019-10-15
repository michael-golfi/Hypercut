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

  def stats(data: GraphFrame) {
    val buckets = data.vertices.select($"id", $"core", $"boundary", lit(k).as("k")).as[BoundaryBucket]
    println("Bucket stats")
    buckets.map(_.coreStats).describe().show
    data.degrees.describe().show
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

    //idGraph
    refineEdges(idGraph)
  }


  /**
   * By looking at the boundary, output unitigs that are ready
   * and then split each bucket into maximal disjoint parts.
   */
  def seizeUnitigsAtBoundary(graph: GraphFrame): DataFrame = {
    //Send all data to all bucket neighbors so that buckets can identify their own boundary and
    //split themselves
    val withBoundary = graph.aggregateMessages.
      sendToDst(struct(idSrc, bndSrc)).
      sendToSrc(struct(idDst, bndDst)).
      agg(collect_list(AM.msg).as("surrounding"))

    val withBuckets = withBoundary.join(graph.vertices, Seq("id"), "left_outer")

    val k = this.k
    //Identify boundary and output/remove unitigs
    //Returns cores split into subparts based on remaining data
    val mergeData = withBuckets.selectExpr("id", "false as centre", "core as coreData",
      "boundary as coreBoundary", "surrounding").as[MergingBuckets].
      map(seizeUnitigs(k)(_)).cache

    val ml = minLength
    mergeData.selectExpr("explode(_1)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    mergeData.selectExpr("_2")
  }

  /**
   * Generate new IDs for each vertex, and a new graph structure,
   * after the buckets have been split.
   */
  def graphWithNewIds(mergeData: DataFrame): GraphFrame = {
    //Assign new IDs to the split core parts
    val preBuckets = mergeData.selectExpr("explode(_2)", "monotonically_increasing_id() as id").
      cache

    //old ID -> newly gen ID
    val relabelVerts = preBuckets.select("col._1", "id")

    //New edges in old ID space
    val newNeighbors = preBuckets.selectExpr("col._1", "explode(col._3)")
    //in new ID space
    val relabelNeighbors = translateAndNormalizeEdges(newNeighbors, relabelVerts)

    val k = this.k

    val nextGraph = GraphFrame(preBuckets.select($"id", $"col._2".as("core")).cache,
      relabelNeighbors.cache)

    if (showStats) {
      println("End of graphWithNewIds")
      stats(nextGraph)
    }
    materialize(nextGraph.edges)
    materialize(nextGraph.vertices)
    preBuckets.unpersist
    nextGraph
  }

  /**
   * Speculatively join buckets with their neighborings based on minimum ID.
   * This enables larger unitigs to be identified and removed in the next iteration.
   */
  def aggregateAtMinNeighbor(graph: GraphFrame) = {

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    val minIds = graph.aggregateMessages.
      sendToDst(idDst).sendToSrc(idSrc).
      sendToDst(idSrc).sendToSrc(idDst).
      agg(min(AM.msg).as("minId"))

    val aggVerts = graph.vertices.join(minIds, Seq("id")).cache

    //Note that data duplication is expected here. A node can send its data as a boundary but
    //still be used as a merge core by other nodes.
    val mergeBoundary = GraphFrame(aggVerts, graph.edges).
      aggregateMessages.
      sendToDst(when(idDst === AM.src("minId"), struct(idSrc, coreSrc, bndSrc))).
      sendToSrc(when(idSrc === AM.dst("minId"), struct(idDst, coreDst, bndDst))).
      agg(collect_list(AM.msg).as("surrounding"))


    val withBuckets2 = mergeBoundary.
      join(aggVerts, Seq("id"), "right_outer").
      selectExpr("id", "(minId == id) as centre", "core", "surrounding")

    val k = this.k
    if (showStats) {
      println("MergingBuckets")
      withBuckets2.as[MergingBuckets].map(_.stats(k)).describe().show
    }

    val r = materialize(withBuckets2.as[MergingBuckets].
      flatMap(simpleMerge(k)(_)).cache)
    aggVerts.unpersist
    r
  }

  /**
   * Run one merge iteration, output data, remove sequences from buckets and
   * merge them.
   */
  def merge(graph: IterationData): IterationData = {
    if (showStats) {
      println("Start of merge")
      stats(graph)
    }

    val mergeData = seizeUnitigsAtBoundary(graph)
    val nextGraph = graphWithNewIds(mergeData)
    val neighborAggregated = aggregateAtMinNeighbor(nextGraph)

    val nextBuckets2 = neighborAggregated.selectExpr("_1", "_2").toDF("id", "bucket")
    val nextEdges2 = translateAndNormalizeEdges(nextGraph.edges, neighborAggregated.selectExpr("explode(_3)", "_1"))
    val nextGraph2 = GraphFrame(materialize(removeLineage(nextBuckets2.select($"id", $"bucket.core",
      $"bucket.boundary", lit(k)))),
      materialize(removeLineage(nextEdges2)))

    mergeData.unpersist
    neighborAggregated.unpersist
    graph.unpersist
    nextGraph.unpersist

    nextGraph2
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
   * treated as if they have no neighbors/unknown boundary.
   */
  def finishBuckets(graph: GraphFrame) {
    val isolated = graph.vertices.select($"id", $"core", lit(k).as("k")).as[BoundaryBucket]
    val ml = minLength
    val unitigs = isolated.flatMap(i => {
      val unitigs = BoundaryBucket.seizeUnitigsAndMerge(i, List())
      unitigs._1.flatMap(lengthFilter(ml)).map(formatUnitig)
    }).write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false
  }

  //begin work in progress

  /**
   * Starting from a graph where edges represent potential bucket intersection,
   * construct a new one where only edges corresponding to actual intersection
   * remain.
   */

  def refineEdges(graph: GraphFrame): GraphFrame = {
    val boundaryOnly = graph.vertices.select("id", "boundary")
    //join edges with boundary data
    val bySrc = graph.edges.toDF("id", "dst").
      join(boundaryOnly.toDF("dst", "dstBoundary"), Seq("dst")).
      groupBy("id").
      agg(collect_list(struct("dst", "dstBoundary")).as("dstData")).
      as[(Long, Array[(Long, Array[String])])]

    val remEdges = graph.vertices.join(bySrc, Seq("id")).as[EdgeData].
      flatMap(d => {
        val finder = BoundaryBucket.overlapFinder(d.boundary, d.k)
        val remain = d.dstData.filter(x => finder.check(x._2))
        remain.map(r => (d.id, r._1))
      })

    GraphFrame(graph.vertices, remEdges.toDF("src", "dst").cache)
  }
  //end work in progress

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame) {
    val dtf = new SimpleDateFormat("HH:mm:ss")
    println(s"[${dtf.format(new Date)}] Initialize iterative merge")

    var data = initialize(graph)
    var n = data.edges.count

    while (n > 0) {
      println(s"[${dtf.format(new Date)}] Begin iteration $iteration ($n edges)")

      data = merge(data)
      n = data.edges.count
      iteration += 1
    }
    println(s"[${dtf.format(new Date)}] No edges left, finishing")
    finishBuckets(data)
    println(s"[${dtf.format(new Date)}] Iterative merge finished")
  }
}

/**
 * A group of buckets (a centre and its neighbors) ready for merging.
 * @param centre true if and only if all the surrounding neighbors could be gathered here.
 *               If false, only some of them are present.
 */
final case class MergingBuckets(id: Long, centre: Boolean, coreData: Array[String], coreBoundary: Array[String],
                                surrounding: Array[(Long, Array[String], Array[String])]) {

  def surroundingData = surrounding.flatMap(_._2)
  def allBoundary = coreBoundary ++ surrounding.flatMap(_._3)
  def removeFrom(from: List[String], k: Int) =
    BoundaryBucket.removeKmers(coreData.iterator ++ coreBoundary.iterator, from, k)

  def stats(k: Int) =
    BoundaryBucketStats(coreData.size,
      coreData.map(Read.numKmers(_, k)).sum,
      allBoundary.size,
      allBoundary.map(Read.numKmers(_, k)).sum)
}

case class EdgeData(id: Long, boundary: Array[String],
  dstData: Array[(Long, Array[String])], k: Int)

/**
 * Helper routines available for serializable functions.
 */
object IterativeSerial {
  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }

  def seizeUnitigs(k: Int)(bkts: MergingBuckets) = {
    val main = BoundaryBucket(bkts.id, bkts.coreData, bkts.coreBoundary, k)
    val surround = bkts.surrounding.map(b => {
      val noDupCore = BoundaryBucket.removeKmers(main.coreAndBoundary, b._2.toList, k)
      val noDupBound = BoundaryBucket.removeKmers(main.coreAndBoundary, b._3.toList, k)
      BoundaryBucket(b._1, noDupCore.toArray, noDupBound.toArray, k)
    }).toList
    val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndMerge(main, surround)
    (unitigs, parts.map(p => (bkts.id, p._1, p._2)))
  }

  def nonMergeResult(id: Long, k: Int, core: Array[String], boundary: Array[String]) =
    (id, BoundaryBucket(id, core, boundary, k), Seq(id))

  /**
   * Merge neighboring buckets together.
   * The boundary of the centre can be promoted to core only if all neighbors could be gathered.
   * Surrounding cores remain core, surrounding boundaries remain boundary.
   * @param k
   * @param bkts
   * @return
   */
  def simpleMerge(k: Int)(bkts: MergingBuckets) = {
    if (bkts.surrounding.isEmpty) {
      if (bkts.centre) {
        //minId node that did not receive anything (neighbours were moved elsewhere)
        Seq(nonMergeResult(bkts.id, k, bkts.coreData, bkts.coreBoundary))
      } else {
        Seq()
      }
    } else {
      val surround = bkts.surrounding

      //some of the surrounding buckets may not truly connect with the core
      //Check boundary against boundary
      val finder  = BoundaryBucket.overlapFinder(bkts.coreBoundary, k)
      val (overlap, noOverlap) = surround.partition(s => finder.check(s._3))

      val newBucket = if (bkts.centre) {
        val newCore = bkts.coreData ++ bkts.coreBoundary ++ bkts.removeFrom(overlap.flatMap(_._2).toList, k)
        val newBoundary = bkts.removeFrom(overlap.flatMap(_._3).toList, k)
        BoundaryBucket(bkts.id, newCore, newBoundary.toArray, k)
      } else {
        val newCore = bkts.coreData ++ bkts.removeFrom(overlap.flatMap(_._2).toList, k)
        val newBoundary = bkts.coreBoundary ++ bkts.removeFrom(overlap.flatMap(_._3).toList, k)
        BoundaryBucket(bkts.id, newCore, newBoundary, k)
      }

      val mergedBucket = (bkts.id, newBucket,
        Seq(bkts.id) ++ overlap.map(_._1))
      val nonMergeBuckets = noOverlap.map(n => nonMergeResult(n._1, k, n._2, n._3))
      Seq(mergedBucket) ++ nonMergeBuckets
    }
  }
}