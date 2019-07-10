package dbpart.spark

import org.graphframes.GraphFrame
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.Pregel
import org.graphframes.lib.AggregateMessages

class IterativeMerge(spark: SparkSession, showStats: Boolean = false) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import SerialRoutines._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  /**
   * Graph where vertices contain IDs only, and a DataFrame that maps IDs to BoundaryBucket.
   */
  type IterationData = (GraphFrame, DataFrame)

  /**
   * Trick to get around the very large execution plans that arise
   * when running iterative algorithms on DataFrames.
   */
  def removeLineage(df: DataFrame) =
    AggregateMessages.getCachedDataFrame(df)

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
  def initialize(graph: GraphFrame, k: Int): IterationData = {
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
  def merge(data: IterationData, k: Int,
            minLength: Option[Int], output: String, append: Boolean): IterationData = {
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

    val mergeData = withBuckets.as[(Long, Long, BoundaryBucket)].
      groupByKey(_._2).mapGroups((key, rows) => { //Group by partition
        //SCB is boundary if partition != coreId
        //TODO: we might get groups here where the core element is missing
        //(has merged with something else) - handle properly

        val data = rows.toList
        val everything = BoundaryBucket(key,
          data.map(r => (r._3.core, r._1 != r._2)),
          k)
        BoundaryBucket.seizeUnitigsAndMerge(everything,
          data.filter(r => r._1 != r._2).map(_._3))
      }).cache

     val writeMode = if (append) "append" else "overwrite"

     mergeData.flatMap(_._1.flatMap(lengthFilter(minLength))).map(u =>
      (u.seq, u.stopReasonStart, u.stopReasonEnd)).
      write.mode(writeMode).csv(s"${output}_unitigs")

     val nextBuckets =
       materialize(removeLineage(mergeData.selectExpr("explode(_2)").selectExpr("col.id", "col").toDF("id", "bucket")))

     //TODO prevent non-merged boundary buckets from merging back with the same core

     val relabelSrc =
       mergeData.selectExpr("explode(_3)").selectExpr("col._1", "col._2").toDF("src", "newSrc")
     val relabelDst = relabelSrc.toDF("dst", "newDst")
     val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].filter(x => x._1 != x._2).toDF("src", "dst").distinct

     //Truncate RDD lineage
     val nextGraph = GraphFrame.fromEdges(materialize(removeLineage(relabelEdges)))

     mergeData.unpersist
     partitions.unpersist
     graph.unpersist
     buckets.unpersist

    (nextGraph, nextBuckets)
  }

  /**
   * Run iterative merge until completion.
   */
  def iterate(graph: GraphFrame, k: Int, minLength: Option[Int], output: String) {
    var data = initialize(graph, k)
    var i = 1
    var n = data._1.edges.count
    while (n > 0) {
      println(s"Begin iteration $i ($n edges)")
      val append = if (i == 1) false else true
      data = merge(data, k, minLength, output, append)
      n = data._1.edges.count
      i += 1
    }
  }
}