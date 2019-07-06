package dbpart.spark

import org.graphframes.GraphFrame
import dbpart.bucket.BoundaryBucket
import org.apache.spark.sql.SparkSession
import org.graphframes.lib.Pregel
import org.graphframes.lib.AggregateMessages

class IterativeMerge(spark: SparkSession) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._
  import SerialRoutines._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  /**
   * Graph where vertices contain IDs only, and a DataFrame that maps IDs to BoundaryBucket.
   */
  type IterationData = (GraphFrame, DataFrame)

  def removeLineage(df: DataFrame) = 
    AggregateMessages.getCachedDataFrame(df)
  
  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   */
  def initialize(graph: GraphFrame, k: Int): IterationData = {
    val buckets =
      graph.vertices.selectExpr("id as bucketId",
        "struct(array() as core, bucket.sequences as boundary, bucket.k as k)",
        "monotonically_increasing_id() as id").as[(Array[Byte], BoundaryBucket, Long)].
          toDF("bucketId", "sequences", "id")

    val relabelSrc = buckets.selectExpr("bucketId as src", "id as newSrc").cache
    val relabelDst = relabelSrc.toDF("dst", "newDst")
    val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].filter(x => x._1 != x._2).toDF("src", "dst").distinct

    val nextBuckets = buckets.select("id", "sequences")
    val idGraph = GraphFrame.fromEdges(relabelEdges.checkpoint)
    relabelSrc.unpersist

    (idGraph, nextBuckets)
  }

  /**
   * Runs one merge iteration, output data, remove sequences from buckets and
   * merge them.
   */
  def merge(data: IterationData, k: Int,
            minLength: Option[Int], output: String, append: Boolean): IterationData = {
    val (graph, buckets) = data

    val partitions = graph.pregel.
      withVertexColumn("partition", $"id",
        array_min(array(Pregel.msg, $"partition"))).
        sendMsgToDst(Pregel.src("partition")).
        sendMsgToSrc(Pregel.dst("partition")).
        aggMsgs(min(Pregel.msg)).
        setMaxIter(1).
        run

    val relabelSrc = partitions.selectExpr("id as src", "partition as newSrc").cache
    val relabelDst = relabelSrc.toDF("dst", "newDst")
    val relabelEdges = graph.edges.join(relabelSrc, Seq("src")).join(relabelDst, Seq("dst")).
      selectExpr("newSrc", "newDst").
      as[(Long, Long)].filter(x => x._1 != x._2).toDF("src", "dst").distinct

    val withBuckets = partitions.join(buckets, Seq("id"))

    val unitigs = withBuckets.as[(Long, Long, BoundaryBucket)].
      groupByKey(_._2).mapGroups((key, rows) => { //Group by partition

        //SCB is boundary if partition != coreId
        (key, BoundaryBucket(
          rows.map(r => (r._3.core ++ r._3.boundary, r._1 != r._2)).toList,
          k).seizeUnitigs)
      })

     val writeMode = if (append) "append" else "overwrite"

     unitigs.flatMap(_._2._1.flatMap(lengthFilter(minLength))).map(u =>
      (u.seq, u.stopReasonStart, u.stopReasonEnd)).
      write.mode(writeMode).csv(s"${output}_unitigs")

     val nextBuckets = 
       removeLineage(unitigs.select("_1", "_2._2").toDF("id", "bucket")).
       checkpoint

     //Truncate RDD lineage
     val nextGraph = GraphFrame.fromEdges(removeLineage(relabelEdges).checkpoint())

     relabelSrc.unpersist
     partitions.unpersist
     graph.unpersist()
     buckets.unpersist()

    (nextGraph, nextBuckets)
  }

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

  /**
   * Unpersist remaining data after the final iteration
   */
  def unpersist {

  }

}