package dbpart.spark

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
   * Run one merge iteration, output data, remove sequences from buckets and
   * merge them.
   */
  def merge(graph: IterationData): IterationData = {
    if (showStats) {
      stats(graph)
    }

    /**
     * Finish buckets with no neighbors
     */
    finishIsolatedBuckets(graph)

    val AM = AggregateMessages
    val idSrc = AM.src("id")
    val idDst = AM.dst("id")
    val coreSrc = AM.src("core")
    val coreDst = AM.dst("core")

    //    println(graph.edges.as[(Long, Long)].collect().toList.groupBy(_._1))

    //Send all data to all bucket neighbors so that buckets can identify their own boundary and
    //split themselves
    val withBoundary = graph.aggregateMessages.
      sendToDst(struct(idSrc, coreSrc)).
      sendToSrc(struct(idDst, coreDst)).
      agg(collect_list(AM.msg).as("boundary"))

    //    Will drop isolated buckets here

    val withBuckets = withBoundary.join(graph.vertices, Seq("id"))

    val k = this.k

    //Identify boundary and output/remove unitigs
    //Returns cores split into subparts based on remaining data
    val mergeData = withBuckets.select("id", "core", "boundary").as[MergingBuckets].
      map(seizeUnitigs(k)(_)).cache

    val ml = minLength
    mergeData.selectExpr("explode(_1)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${output}_unitigs")
    firstWrite = false

    //Assign new IDs to the split core parts
    val preBuckets = mergeData.selectExpr("explode(_2)", "monotonically_increasing_id() as id").
      cache

    //old ID -> newly gen ID
    val relabelVerts = preBuckets.select("col._1", "id")

    //New edges in old ID space
    val newNeighbors = preBuckets.selectExpr("col._1", "explode(col._3)")
    //in new ID space
    val relabelNeighbors = translateEdges(newNeighbors, relabelVerts)

    //TODO optimize away map, then possibly avoid caching
    val nextBuckets = preBuckets.select("id", "col._2").as[(Long, Array[String])].map(r => {
      (r._1, BoundaryBucket(r._1, r._2, k))
    }).toDF("id", "bucket")

    val nextGraph = GraphFrame(nextBuckets.select("id", "bucket.core").cache,
      relabelNeighbors.cache)

    if (showStats) {
      stats(nextGraph)
    }

    //Figure out the minimum neighboring ID to send neighboring parts to for speculative merge
    val minIds = nextGraph.aggregateMessages.sendToDst(idSrc).
      sendToSrc(idDst).
      agg(min(AM.msg).as("minId"))

    //Some buckets would have a smaller self-ID than minId. minId does not take self-ID into account.
    //Thus we check when sending the message.
    val mergeBoundary = GraphFrame(nextGraph.vertices.join(minIds, Seq("id")).cache, nextGraph.edges).
      aggregateMessages.
      sendToDst(when(idDst === AM.src("minId") && idDst < idSrc, struct(AM.src("id"), AM.src("core")))).
      sendToSrc(when(idSrc === AM.dst("minId") && idSrc < idDst, struct(AM.dst("id"), AM.dst("core")))).
      agg(collect_list(AM.msg).as("boundary"))

    val withBuckets2 = mergeBoundary.join(nextGraph.vertices, Seq("id")).select("id", "core", "boundary")

    //Out of those buckets that were merged together at the minId, figure out which ones actually have an intersection
    //and merge them. Some buckets may not be merged in.
    val mergeData2 = withBuckets2.as[MergingBuckets].
      flatMap(simpleMerge(k)(_)).cache

    val nextBuckets2 = mergeData2.selectExpr("_1", "_2").toDF("id", "bucket")
    val nextEdges2 = translateEdges(nextGraph.edges, mergeData2.selectExpr("explode(_3)", "_1"))
    val nextGraph2 = GraphFrame(materialize(removeLineage(nextBuckets2.selectExpr("id", "bucket.core", "bucket.k"))),
      materialize(removeLineage(nextEdges2)))

    mergeData.unpersist
    preBuckets.unpersist
    mergeData2.unpersist
    graph.unpersist
    mergeBoundary.unpersist
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

  def finishIsolatedBuckets(graph: GraphFrame) {
    val isolated = graph.vertices.join(graph.degrees, Seq("id"), "left_outer").
      filter("degree is null").as[BoundaryBucket]

    //    println(s"${isolated.collect().toList.map(_.id)}")

    if (!isolated.isEmpty) {
      println(s"There are isolated nodes")
      val ml = minLength
      val unitigs = isolated.flatMap(i => {
        val unitigs = BoundaryBucket.seizeUnitigsAndMerge(i, List())
        unitigs._1.flatMap(lengthFilter(ml)).map(formatUnitig)
      }).write.mode(nextWriteMode).csv(s"${output}_unitigs")
      firstWrite = false
    } else {
      println(s"No isolated nodes")
    }
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame) {
    var data = initialize(graph)

    var n = data.edges.count
    while (n > 0) {
      println(s"Begin iteration $iteration ($n edges)")

      data = merge(data)
      n = data.edges.count
      iteration += 1
    }
    println("No edges left, finishing")
    finishIsolatedBuckets(data)
  }
}

/**
 * A group of buckets (a center and its neighbors) ready for merging.
 */
final case class MergingBuckets(id: Long, core: Array[String],
                                boundary: Array[(Long, Array[String])])
object IterativeSerial {
  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }

  def seizeUnitigs(k: Int)(bkts: MergingBuckets) = {
    val main = BoundaryBucket(bkts.id, bkts.core, k)
    val surround = bkts.boundary.map(b => {
      val noDupCore = BoundaryBucket.withoutDuplicates(main.kmers.iterator, b._2.toList, k)
      BoundaryBucket(b._1, noDupCore.toArray, k)
    }).toList
    val (unitigs, parts) = BoundaryBucket.seizeUnitigsAndMerge(main, surround)
    (unitigs, parts.map(p => (bkts.id, p._1, p._2)))
  }

  def noDuplicateMerge(k: Int)(bkts: MergingBuckets) = {
    val main = BoundaryBucket(bkts.id, bkts.core, k)
    val surround = bkts.boundary.flatMap(_._2)
    (main.id,
      main.copy(core = main.core ++ BoundaryBucket.withoutDuplicates(main.kmers.iterator, surround.toList, k)))
  }

  def simpleMerge(k: Int)(bkts: MergingBuckets) = {
    if (bkts.boundary.isEmpty) {
      //Nothing was sent here
      Seq()
    } else {
      val boundMap = bkts.boundary.groupBy(_._1)
      //Take the first surround bucket for each ID key
      val surround = boundMap.values.map(_.head)

      //At this point, some of the surrounding buckets may not truly connect with the core
      val main = BoundaryBucket(bkts.id, bkts.core, k)
      val mainIn = main.prefixSet
      val mainOut = main.suffixSet
      val (overlap, noOverlap) = surround.partition(s => BoundaryBucket.overlapsWith(s._2, mainOut, mainIn, k))

      val allData = bkts.core ++ overlap.flatMap(_._2)
      val km = allData.flatMap(Read.kmers(_, k)).toList

      //TODO figure out why this assertion fails, then remove distinct call below
//      assert(km.distinct.size == km.size)

      val mergedBucket = (bkts.id,
        BoundaryBucket(bkts.id, km.distinct.toArray, k),
        Seq(bkts.id) ++ overlap.map(_._1))
      val nonMergeBuckets = noOverlap.map(n =>  (n._1, BoundaryBucket(n._1, n._2, k), Seq(n._1)))
      Seq(mergedBucket) ++ nonMergeBuckets
    }
  }
}