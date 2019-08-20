package dbpart.spark

import org.graphframes.GraphFrame
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.Pregel
import org.graphframes.lib.AggregateMessages
import org.graphframes.pattern.Edge
import dbpart.Contig

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
  type IterationData = (GraphFrame, DataFrame)

  var iteration = 1
  val checkpointInterval = 4

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

  def stats(buckets: Dataset[BoundaryBucket]) {
    println("Core stats")
    buckets.map(_.coreStats).describe().show
  }

  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   */
  def initialize(graph: GraphFrame): IterationData = {
    val buckets =
      graph.vertices.selectExpr("id as bucketId",
        "struct(monotonically_increasing_id() as id, bucket.sequences as core, bucket.k as k, 0 as generation)"
        ).as[(Array[Byte], BoundaryBucket)].
          toDF("bucketId", "bucket")

    val relabelSrc = buckets.selectExpr("bucketId as src", "bucket.id as newSrc").cache
    val relabelDst = relabelSrc.toDF("dst", "newDst")
    val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].filter(x => x._1 != x._2).toDF("src", "dst").distinct

      //TODO this duplicates data
    val nextBuckets = buckets.selectExpr("bucket.id as id", "bucket")
//    val idGraph = GraphFrame.fromEdges(relabelEdges.cache)
    val idGraph = GraphFrame.apply(buckets.selectExpr("bucket.id as id", "0 as generation").cache,
      relabelEdges.cache)
    materialize(relabelEdges)
    relabelSrc.unpersist

    (idGraph, nextBuckets)
  }

  /**
   * Runs one merge iteration, output data, remove sequences from buckets and
   * merge them.
   *
   * At each iteration step, we run pregel to identify neighbors to join based on
   * minimum ID, then merge BoundaryBuckets together, output and remove as much
   * sequence as possible. A new, smaller graph and a new set of merged buckets
   * is produced for the next iteration.
   */
  def merge(data: IterationData, append: Boolean): IterationData = {
    val (graph, buckets) = data

    if (showStats) {
      stats(buckets.select("bucket.*").as[BoundaryBucket])
    }

    finishIsolatedBuckets(data, append)

    val genMsg = s"${Pregel.msg}.generation"
    val partMsg = s"${Pregel.msg}.id"
    val idSrc = Pregel.src("id")
    val idDst = Pregel.dst("id")
    val genSrc = Pregel.src("generation")
    val genDst = Pregel.dst("generation")

//    println(graph.edges.as[(Long, Long)].collect().toList.groupBy(_._1))

    //Inner join on vertices with degrees will drop isolated buckets here
    val partitions = GraphFrame(
        graph.vertices.join(graph.degrees, Seq("id")),
        graph.edges).pregel.
        withVertexColumn("partition", $"id",
        expr(s"IF ($genMsg < generation OR ($genMsg == generation AND $partMsg < partition), $partMsg, partition)")).
      sendMsgToDst(expr(s"struct($genSrc, $idSrc)")).
      sendMsgToSrc(expr(s"struct($genDst, $idDst)")).
      aggMsgs(min(Pregel.msg)).
      setMaxIter(1).
      run

    /**
     * Challenge: case where one merge center is torn between two different
     * merge groups. To ensure progress over time, we prefer the smaller
     * merge group of the two.
     *
     * E.g. edges: 5 - 3 - 2 - (4, 6)
     * 3 would initially be chosen to merge into (3, 2, 4, 6) by min bucket id,
     * but (5, 3, 2) is smaller by size so 2 should get to merge into 3 instead.
     * This happens between two connecting centers (2 and 3 in this example).
     */

    val withBuckets = partitions.join(buckets, Seq("id"))

    val k = this.k
    val mergeData = withBuckets.as[MergingBuckets].
      groupByKey(_.partition).mapGroups(seizeUnitigs(k)(_, _)). //Group by partition
       cache

     val writeMode = "append"

     val ml = minLength
     mergeData.flatMap(_._1.flatMap(lengthFilter(ml))).map(u => formatUnitig(u)).
      write.mode(writeMode).csv(s"${output}_unitigs")

     val nextBuckets =
       materialize(removeLineage(mergeData.selectExpr("explode(_2)").selectExpr("col.id", "col").toDF("id", "bucket")))

     val relabelSrc =
       mergeData.selectExpr("explode(_3)").selectExpr("col._1", "col._2").toDF("src", "newSrc")
     val removableEdges =
       mergeData.selectExpr("explode(_4)").selectExpr("col._1", "col._2").as[(Long, Long)]
     val relabelDst = relabelSrc.toDF("dst", "newDst")
     val relabelEdges = graph.edges.as[(Long, Long)].except(removableEdges).
       join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
        selectExpr("newSrc", "newDst").as[(Long, Long)].
        filter(x => x._1 != x._2).toDF("src", "dst").distinct

     //Truncate RDD lineage
     val nextGraph = GraphFrame(
       nextBuckets.selectExpr("id", "bucket.generation as generation").cache,
       materialize(removeLineage(relabelEdges))
       )

     mergeData.unpersist
     partitions.unpersist
     graph.unpersist
     graph.edges.rdd.unpersist(true)
     buckets.rdd.unpersist(true)

    (nextGraph, nextBuckets)
  }

  def finishIsolatedBuckets(data: IterationData, append: Boolean) {
    val (graph, buckets) = data

    val isolated = buckets.join(graph.degrees, Seq("id"), "left_outer").
      filter("degree is null").selectExpr("id", "bucket.core", "bucket.k", "bucket.generation").as[BoundaryBucket]

    val writeMode = if (append) "append" else "overwrite"
    println(s"${isolated.count()} isolated nodes")
//    println(s"${isolated.collect().toList.map(_.id)}")

    val ml = minLength
    val unitigs = isolated.flatMap(i => {
      val unitigs = BoundaryBucket.seizeUnitigsAndMerge(i, List())
      unitigs._1.flatMap(lengthFilter(ml)).map(u =>
        (u.seq, u.stopReasonStart, u.stopReasonEnd))
    }).write.mode(writeMode).csv(s"${output}_unitigs")
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame) {
    var data = initialize(graph)

    var n = data._1.edges.count
    while (n > 0) {
      println(s"Begin iteration $iteration ($n edges)")
//      println(data._1.edges.as[(Long, Long)].collect.toList)
      val append = (iteration > 1)

      data = merge(data, append)
      n = data._1.edges.count
      iteration += 1
    }
    finishIsolatedBuckets(data, iteration > 1)
  }
}

/**
 * A group of buckets (a center and its neighbors) ready for merging.
 */
final case class MergingBuckets(id: Long, degree: Long, partition: Long,
  bucket: BoundaryBucket) {
  def isCenter = id == partition
}

object IterativeSerial {
  def formatUnitig(u: Contig) = {
    (u.seq, u.stopReasonStart, u.stopReasonEnd, u.length + "bp", (u.length - u.k + 1) + "k")
  }
  
  def seizeUnitigs(k: Int)(key: Long, rows: Iterator[MergingBuckets]) = {
    //is boundary if partition != coreId

    val data = rows.toList
    val (core, boundary) = data.partition(_.isCenter)
    assert(core.size <= 1)
    val hasCore = core.size == 1

    if (hasCore) {
      val deg = core.head.degree
      val hasFullBoundary = (core.head.degree == data.size - 1)
      if (hasFullBoundary) {
        println(s"Full merge $key $deg ${data.map(_.id)}")

        //Have the core and all the boundary buckets as part of the merge
        BoundaryBucket.seizeUnitigsAndMerge(core.head.bucket, boundary.map(_.bucket))
      } else {
        println(s"Missing boundary $key $deg ${data.map(_.id)}")
        //Some of the boundary is missing.
        //Merge buckets but do not remove any data.
        //TODO can this lead to huge unprocessed buckets?
        BoundaryBucket.simpleMerge(key, k, data.map(_.bucket))
//        BoundaryBucket.identityMerge(data.map(_.bucket))
      }
    } else {
        println(s"Missing core $key ${data.map(_.id)}")
        //the desired core element is missing since it merged with something else.
        //Pick an arbitrary ID from the group and merge without removing data.
        //TODO can this lead to huge unprocessed buckets?
      BoundaryBucket.simpleMerge(data.head.id, k, data.map(_.bucket))
//      BoundaryBucket.identityMerge(data.map(_.bucket))
    }
  }
}