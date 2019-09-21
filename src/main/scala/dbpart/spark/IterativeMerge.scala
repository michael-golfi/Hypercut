package dbpart.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.graphframes.GraphFrame
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.AggregateMessages
import dbpart.Contig
import dbpart.shortread.Read

class IterativeMerge(spark: SparkSession, showStats: Boolean = false,
  minLength: Option[Int], k: Int, output: String) {

  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import SerialRoutines._
  import IterativeSerial._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  /**
   * Graph where vertices contain IDs only, and a DataFrame that maps IDs to BoundaryBucket.
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

  def materialize(df: DataFrame) = {
    df.foreachPartition((r: Iterator[Row]) => () )
    df
  }

  def stats(data: GraphFrame) {
    val buckets = data.vertices.select("bucket.*").as[BoundaryBucket]
    println("Bucket stats")
    buckets.map(_.coreStats).describe().show
    data.degrees.describe().show
  }

  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   */
  def initialize(graph: GraphFrame): IterationData = {
    val buckets =
      graph.vertices.selectExpr("id as bucketId",
        "struct(monotonically_increasing_id() as id, bucket.sequences as core, bucket.k as k)").as[(Array[Byte], BoundaryBucket)].
          toDF("bucketId", "bucket")

    val edgeTranslation = buckets.selectExpr("bucketId", "bucket.id").cache
    val edges = translateEdges(graph.edges, edgeTranslation).cache

    val idGraph = GraphFrame(buckets.selectExpr("bucket.*").cache,
      edges)
    materialize(edges)
    edgeTranslation.unpersist

    idGraph
  }


  /**
   * By looking at the boundary, output unitigs that are ready for output
   * and then split each bucket into disjoint parts.
   */
  def seizeUnitigsAtBoundary(graph: GraphFrame): DataFrame = {
    //Send all data to all bucket neighbors so that buckets can identify their own boundary and
    //split themselves
    val withBoundary = graph.aggregateMessages.
      sendToDst(struct(idSrc, coreSrc)).
      sendToSrc(struct(idDst, coreDst)).
      agg(collect_list(AM.msg).as("boundary"))

    val withBuckets = withBoundary.join(graph.vertices, Seq("id"), "left_outer")

    val k = this.k
    //Identify boundary and output/remove unitigs
    //Returns cores split into subparts based on remaining data
    val mergeData = withBuckets.selectExpr("id", "false as centre", "core", "boundary").as[MergingBuckets].
      map(seizeUnitigs(k)(_)).cache

    val ml = minLength
    mergeData.selectExpr("explode(_1)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    mergeData.selectExpr("_2")
  }

  def graphWithNewIds(mergeData: DataFrame): GraphFrame = {
    //Assign new IDs to the split core parts
    val preBuckets = mergeData.selectExpr("explode(_2)", "monotonically_increasing_id() as id").
      cache

    //old ID -> newly gen ID
    val relabelVerts = preBuckets.select("col._1", "id")

    //New edges in old ID space
    val newNeighbors = preBuckets.selectExpr("col._1", "explode(col._3)")
    //in new ID space
    val relabelNeighbors = translateEdges(newNeighbors, relabelVerts)

    val k = this.k
    //TODO optimize away map, then possibly avoid caching
    val nextBuckets = preBuckets.select("id", "col._2").as[(Long, Array[String])].map(r => {
      (r._1, BoundaryBucket(r._1, r._2, k))
    }).toDF("id", "bucket")

    val nextGraph = GraphFrame(nextBuckets.select("id", "bucket.core").cache,
      relabelNeighbors.cache)

    if (showStats) {
      stats(nextGraph)
    }
    materialize(nextGraph.edges)
    materialize(nextGraph.vertices)
    preBuckets.unpersist
    nextGraph
  }

  def aggregateAtMinNeighbor(graph: GraphFrame) = {

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    val minIds = graph.aggregateMessages.
      sendToDst(idDst).sendToSrc(idSrc).
      sendToDst(idSrc).sendToSrc(idDst).
      agg(min(AM.msg).as("minId"))

    val aggVerts = graph.vertices.join(minIds, Seq("id")).cache

    //Some buckets would have a smaller self-ID than minId. minId does not take self-ID into account.
    //Thus we check when sending the message.
    //Note that data duplication may happen here. A node can send its data as a boundary but
    //still be used as a merge core by other nodes.
    val mergeBoundary = GraphFrame(aggVerts, graph.edges).
      aggregateMessages.
      sendToDst(when(idDst === AM.src("minId"), struct(AM.src("id"), AM.src("core")))).
      sendToSrc(when(idSrc === AM.dst("minId"), struct(AM.dst("id"), AM.dst("core")))).
      agg(collect_list(AM.msg).as("boundary"))

    val withBuckets2 = mergeBoundary.
      join(aggVerts, Seq("id"), "right_outer").
      selectExpr("id", "(minId == id) as centre", "core", "boundary")

    materialize(withBuckets2)
    aggVerts.unpersist
    val k = this.k
    //Out of those buckets that were merged together at the minId, figure out which ones actually have an intersection
    //and merge them. Some buckets may not be merged in.
    withBuckets2.as[MergingBuckets].
      flatMap(simpleMerge(k)(_)).cache
  }

  /**
   * Run one merge iteration, output data, remove sequences from buckets and
   * merge them.
   */
  def merge(graph: IterationData): IterationData = {
    if (showStats) {
      stats(graph)
    }

    val mergeData = seizeUnitigsAtBoundary(graph)
    val nextGraph = graphWithNewIds(mergeData)
    val neighborAggregated = aggregateAtMinNeighbor(nextGraph)

    val nextBuckets2 = neighborAggregated.selectExpr("_1", "_2").toDF("id", "bucket")
    val nextEdges2 = translateEdges(nextGraph.edges, neighborAggregated.selectExpr("explode(_3)", "_1"))
    val nextGraph2 = GraphFrame(materialize(removeLineage(nextBuckets2.selectExpr("id", "bucket.core", "bucket.k"))),
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
   */
  def translateEdges(edges: DataFrame, translation: DataFrame) = {
      edges.toDF("src", "dst").
        join(translation.toDF("src", "newSrc"), Seq("src")).
        join(translation.toDF("dst", "newDst"), Seq("dst")).
        selectExpr("newSrc", "newDst").toDF("src", "dst").distinct().
        filter("!(src == dst)")
  }

  def finishBuckets(graph: GraphFrame) {
    val isolated = graph.vertices.as[BoundaryBucket]
    val ml = minLength
    val unitigs = isolated.flatMap(i => {
      val unitigs = BoundaryBucket.seizeUnitigsAndMerge(i, List())
      unitigs._1.flatMap(lengthFilter(ml)).map(formatUnitig)
    }).write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false
  }

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
 * A group of buckets (a center and its neighbors) ready for merging.
 */
final case class MergingBuckets(id: Long, centre: Boolean, core: Array[String],
                                boundary: Array[(Long, Array[String])])

object IterativeSerial {
  val AM = AggregateMessages
  val idSrc = AM.src("id")
  val idDst = AM.dst("id")
  val coreSrc = AM.src("core")
  val coreDst = AM.dst("core")

  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }

  def seizeUnitigs(k: Int)(bkts: MergingBuckets) = {
    val main = BoundaryBucket(bkts.id, bkts.core, k)
    val surround = bkts.boundary.map(b => {
      //TODO is this safe?
       val noDupCore = BoundaryBucket.withoutDuplicates(main.core.iterator,
              b._2.flatMap(Read.kmers(_, k)).toList, k)
//      BoundaryBucket(b._1, b._2, k)
        BoundaryBucket(b._1, noDupCore.toArray, k)
    }).toList
    val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndMerge(main, surround)
    (unitigs, parts.map(p => (bkts.id, p._1, p._2)))
  }

  def nonMergeResult(id: Long, k: Int, sequences: Array[String]) =
    (id, BoundaryBucket(id, sequences, k), Seq(id))

  def simpleMerge(k: Int)(bkts: MergingBuckets) = {
    if (bkts.boundary.isEmpty) {
      if (bkts.centre) {
        //minId node that did not receive anything (neighbours were moved elsewhere)
        Seq(nonMergeResult(bkts.id, k, bkts.core))
      } else {
        Seq()
      }
    } else {
      val boundMap = bkts.boundary.groupBy(_._1)
      //Take the first surround bucket for each ID key
      val surround = boundMap.values.map(_.head)

      //At this point, some of the surrounding buckets may not truly connect with the core
      val main = BoundaryBucket(bkts.id, bkts.core, k)
      val mainIn = main.purePrefixSet
      val mainOut = main.pureSuffixSet
      val mainInAndOut = main.prefixAndSuffixSet
      val (overlap, noOverlap) = surround.partition(s =>
        BoundaryBucket.overlapsWith(mainInAndOut, mainOut, mainIn, k, s._2))

      val allData = bkts.core ++ overlap.flatMap(_._2)
      val km = allData.flatMap(Read.kmers(_, k)).toList

      //Note that allData may contain duplicate k-mers at this point - see above
      val mergedBucket = (bkts.id,
        BoundaryBucket(bkts.id, allData, k),
        Seq(bkts.id) ++ overlap.map(_._1))
      val nonMergeBuckets = noOverlap.map(n => nonMergeResult(n._1, k, n._2))
      Seq(mergedBucket) ++ nonMergeBuckets
    }
  }
}