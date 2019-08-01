package dbpart.spark

import org.graphframes.GraphFrame
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.Pregel
import org.graphframes.lib.AggregateMessages

class IterativeMerge(spark: SparkSession, showStats: Boolean = false,
  k: Int, output: String) {

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
    println("Boundary stats")
    buckets.map(_.boundaryStats).describe().show
  }

  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   */
  def initialize(graph: GraphFrame): IterationData = {
    val buckets =
      graph.vertices.selectExpr("id as bucketId",
        "struct(monotonically_increasing_id() as id, bucket.sequences as core, array() as boundary, bucket.k as k)"
        ).as[(Array[Byte], BoundaryBucket)].
          toDF("bucketId", "bucket")

    val relabelSrc = buckets.selectExpr("bucketId as src", "bucket.id as newSrc").cache
    val relabelDst = relabelSrc.toDF("dst", "newDst")
    val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].filter(x => x._1 != x._2).toDF("src", "dst").distinct

      //TODO this duplicates data
    val nextBuckets = buckets.selectExpr("bucket.id as id", "bucket")
    val idGraph = GraphFrame.fromEdges(relabelEdges.cache)
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
  def merge(data: IterationData,
            minLength: Option[Int], append: Boolean): IterationData = {
    val (graph, buckets) = data

    if (showStats) {
      stats(buckets.select("bucket.*").as[BoundaryBucket])
    }

    val partitions = graph.pregel.
      withVertexColumn("partition", $"id",
        array_min(array(Pregel.msg, $"partition"))).
        sendMsgToDst(Pregel.src("partition")).
        sendMsgToSrc(Pregel.dst("partition")).
        aggMsgs(min(Pregel.msg)).
        setMaxIter(1).
        run

    val withBuckets = partitions.join(buckets, Seq("id"))

    val k = this.k
    val mergeData = withBuckets.as[(Long, Long, BoundaryBucket)].
      groupByKey(_._2).mapGroups(seizeUnitigs(k)(_, _)). //Group by partition
       cache

     val writeMode = if (append) "append" else "overwrite"

     mergeData.flatMap(_._1.flatMap(lengthFilter(minLength))).map(u =>
      (u.seq, u.stopReasonStart, u.stopReasonEnd)).
      write.mode(writeMode).csv(s"${output}_unitigs")

     val nextBuckets =
       materialize(removeLineage(mergeData.selectExpr("explode(_2)").selectExpr("col.id", "col").toDF("id", "bucket")))

     val relabelSrc =
       mergeData.selectExpr("explode(_3)").selectExpr("col._1", "col._2").toDF("src", "newSrc")
     val removableEdges =
       mergeData.selectExpr("explode(_4)").selectExpr("col._1", "col._2").as[(Long, Long)]
     val relabelDst = relabelSrc.toDF("dst", "newDst")
     val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].except(removableEdges).
      filter(x => x._1 != x._2).toDF("src", "dst").distinct

     //Truncate RDD lineage
     val nextGraph = GraphFrame.fromEdges(materialize(removeLineage(relabelEdges)))

     mergeData.unpersist
     partitions.unpersist
     graph.unpersist
     graph.edges.rdd.unpersist(true)
     buckets.rdd.unpersist(true)

    (nextGraph, nextBuckets)
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame, minLength: Option[Int]) {
    var data = initialize(graph)

    var n = data._1.edges.count
    while (n > 0) {
      println(s"Begin iteration $iteration ($n edges)")
      val append = if (iteration == 1) false else true
      data = merge(data, minLength, append)
      n = data._1.edges.count
      iteration += 1
    }
  }
}

object IterativeSerial {
  def seizeUnitigs(k: Int)(key: Long, rows: Iterator[(Long, Long, BoundaryBucket)]) = {
    //SCB is boundary if partition != coreId
    //TODO: we might get groups here where the core element is missing
    //(has merged with something else) - handle properly

    val data = rows.toList
    val everything = BoundaryBucket(
      key, data.map(r => (r._3.core, r._1 != r._2)),
      k)
    BoundaryBucket.seizeUnitigsAndMerge(
      everything,
      data.filter(r => r._1 != r._2).map(_._3))
  }
}