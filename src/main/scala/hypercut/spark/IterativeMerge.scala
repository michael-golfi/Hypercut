package hypercut.spark

import java.text.SimpleDateFormat
import java.util.Date

import hypercut.Contig
import hypercut.bucket.{BoundaryBucket, MapOverlapFinder, Util}
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

/**
 * Iterative merge process that gradually collapses the graph by merging buckets with neighbors,
 * outputting sequence as we go along.
 */
class IterativeMerge(spark: SparkSession, showStats: Boolean = false,
                     minLength: Option[Int], k: Int, outputLocation: String) {

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

  val PARTS = 1000
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
    val k = this.k
    if (show) {
      println(label)
      val buckets = data.vertices.as[BoundaryBucket]
      buckets.map(_.ops(k).coreStats).describe().show
      data.degrees.describe().show
    }
  }

  def numKmers(data: GraphFrame): Long = {
    val k = this.k
    data.vertices.as[BoundaryBucket].
      map(_.ops(k).coreStats.numKmers).reduce(IterativeSerial.sum(_, _))
  }

  /**
   * Prepare the first iteration.
   * Sets up new ID numbers for each bucket and prepares the iterative merge.
   * Sets all sequences to boundary.
   */
  def initialize(graph: GraphFrame): GraphFrame = {
    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
    //to control partitions for KeyValueGroupedDataset (and others)
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, PARTS)

    //Materialize buckets eagerly to persist monotonically increasing IDs
    val buckets =
      materialize(
      graph.vertices.selectExpr("id as bucketId",
        "struct(monotonically_increasing_id() as id, array() as core, bucket.sequences as boundary)").
        as[(Array[Byte], BoundaryBucket)].
        toDF("bucketId", "bucket").
        cache
      )

    val edgeTranslation = buckets.selectExpr("bucketId", "bucket.id")
    val edges =
      translateAndNormalizeEdges(graph.edges, edgeTranslation).cache

    val idGraph = GraphFrame(materialize(buckets.selectExpr("bucket.*").cache),
      materialize(edges))
    buckets.unpersist

    if (showStats) {
      println(idGraph.vertices.count + " vertices")
      idGraph.degrees.describe().show()
    }

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
      val (unitigs, parts) = b.ops(k).seizeUnitigsAndSplit
      (b.id, unitigs, parts)
    })
    //TODO should splitData be cached?

    val ml = minLength
    splitData.selectExpr("explode(_2)").select("col.*").as[Contig].
      flatMap(c => lengthFilter(ml)(c).map(u => formatUnitig(u))).
      write.mode(nextWriteMode).csv(s"${outputLocation}_unitigs")
    firstWrite = false

    val provisionalParts = materialize(splitData.selectExpr("_1 as id",
      "transform(_3, x -> x.boundary) as boundary",
      "transform(_3, x -> x.core) as core",
      "transform(_3, x -> monotonically_increasing_id()) as newIds").cache)

    val splitBoundary = provisionalParts.select("id", "boundary", "newIds").
      as[(Long, Array[Array[String]], Array[Long])].map(x => {
      (x._1, x._2.map(_.map(BPBuffer.wrap)), x._3)
    }).toDF("id", "boundary", "newIds").as[SplitBoundary].cache

    val builtEdges = edgesFromSplitBoundaries(splitBoundary, broadcast(graph.edges.as[(Long, Long)]))
    val builtBuckets = provisionalParts.selectExpr("explode(arrays_zip(newIds, core, boundary))")
    val nextBuckets = builtBuckets.selectExpr("col.newIds as id", "col.core as core", "col.boundary as boundary").
      as[BoundaryBucket]

    val nb = materialize(removeLineage(nextBuckets.toDF))
    val ne = materialize(removeLineage(builtEdges.toDF("src", "dst", "intersection")))

    provisionalParts.unpersist
    graph.edges.unpersist
    splitBoundary.unpersist
    graph.vertices.unpersist
    shiftBoundary(GraphFrame(nb, ne))
  }

  /**
   * Given split buckets and old edges, compute all the new edges that correspond to actual intersection
   * between the next-iteration split buckets.
   */
  def edgesFromSplitBoundaries(boundaries: Dataset[SplitBoundary], edges: Dataset[(Long, Long)]):
    Dataset[(Long, Long, Array[String])] = {

    val es = edges
    val bs = boundaries
    val joint = es.joinWith(bs, bs("id") === es("dst"))
    val jointSrc = joint.groupByKey(_._1._1)
    val bySrc = bs.groupByKey(_.id)
    val k = this.k
    bySrc.cogroup(jointSrc)((key, it1, it2) => {
      for {
        from <- it1
        to <- it2.map(_._2)
        edge <- from.edgesTo(to, k)
      } yield edge
    })
  }

  //Experimental, alternative to cogroup
  def edgesFromSplitBoundaries3(boundaries: DataFrame, edges: DataFrame): Dataset[(Long, Long, Array[String])] = {
    val graph = GraphFrame(boundaries, edges)
    val k = this.k
    graph.triplets.toDF("_1", "_2", "_3").as[(SplitBoundary, (Long, Long), SplitBoundary)].flatMap(b => {
      b._1.edgesTo(b._3, k)
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
   * After iterations have finished, merge all remaining data into a single bucket and output.
   */
  def finishBuckets(graph: GraphFrame) {
    val buckets = graph.vertices.as[BoundaryBucket]
    val k = this.k
    val ml = minLength
    val all = buckets.collect()
    val joint = BoundaryBucket(0,
      all.flatMap(_.core) ++ all.flatMap(_.boundary),
      Array())
    val unitigs = joint.ops(k).seizeUnitigsAndSplit._1.
      flatMap(lengthFilter(ml)).map(formatUnitig)
    unitigs.toDS.
      write.mode(nextWriteMode).csv(s"${outputLocation}_unitigs")
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
      selectExpr("struct(id, core, boundary) as centre",
        "(if (isnotnull(degree), degree, 0)) as degree",
        "(if (isnotnull(minId), (minId == id), true)) as receiver",
        "surrounding").as[CollectedBuckets].cache

//    if (showStats) {
//      collected.show
//    }

    val r = materialize(collected.flatMap(_.unified(k)).cache)
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
    val boundaryOnly = graph.vertices.selectExpr("id", "array(boundary) as boundary",
      "array(id) as newIds").as[(Long, Array[Array[String]], Array[Long])].map(b => {
      SplitBoundary(b._1, b._2.map(x => x.map(BPBuffer.wrap)), b._3)
    }).cache
      //toDF("id", "boundary", "newIds").cache

    val foundEdges = edgesFromSplitBoundaries(boundaryOnly, graph.edges.as[(Long, Long)])
    val newEdges = materialize(foundEdges.cache).toDF("src", "dst", "intersection")
    graph.edges.unpersist
//    boundaryOnly.unpersist
    GraphFrame(graph.vertices, newEdges)
  }

  def shiftBoundary(graph: GraphFrame): GraphFrame = {
    val k = this.k
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
            case Some(int) => row._1.ops(k).shiftBoundary(int._2.toSeq.flatten)
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
  def iterate(graph: GraphFrame, stopAtKmers: Long) {
    val dtf = new SimpleDateFormat("HH:mm:ss")
    println(s"[${dtf.format(new Date)}] Initialize iterative merge")

    var data = shiftBoundary(refineEdges(initialize(graph)))
    var n = numKmers(data)

    while (n > stopAtKmers) {
      println(s"[${dtf.format(new Date)}] Begin iteration $iteration ($n k-mers)")

      data = runIteration(data)
      n = numKmers(data)
      iteration += 1
    }
    println(s"[${dtf.format(new Date)}] Finishing ($n k-mers)")
    finishBuckets(data)
    println(s"[${dtf.format(new Date)}] Iterative merge finished")
  }
}

final case class SplitBoundary(id: Long, boundary: Array[Array[ZeroBPBuffer]], newIds: Array[Long]) {

  @transient
  lazy val unpacked = boundary.map(b => b.map(_.toString()))


  /*
  The efficiency of looking up boundary intersections between pairs of split buckets
  degrades as the split parts get smaller and smaller, if one set per split part is used for
  overlap finding (linear cost in data size as the size of the split parts approach 1).
  The MapOverlapFinder combats this problem by looking up overlaps across all split parts
  simultaneously from a map.
   */

  private def finder(k: Int) = {
    val data = (unpacked zip newIds).toList.map {
      case (bound, id) => {
        ((Util.prefixesAndSuffixes(bound.iterator, k).toArray, id),
          (Util.purePrefixes(bound.iterator, k).toArray, id),
          (Util.pureSuffixes(bound.iterator, k).toArray, id))
      }
    }

    new MapOverlapFinder(data.map(_._1), data.map(_._2), data.map(_._3), k)
  }

  @transient
  private var cachedFinder: Option[MapOverlapFinder] = None
  def getFinder(k: Int) = cachedFinder match {
    case Some(f) => f
    case None =>
      cachedFinder = Some(finder(k))
      cachedFinder.get
  }

  def edgesTo(other: SplitBoundary, k: Int): Iterator[(Long, Long, Array[String])] = {
    val finder = getFinder(k)
    for {
      (to, toNid) <- other.unpacked.iterator zip other.newIds.iterator
      (overlap, thisId) <- finder.find(to)
      edge = (thisId, toNid, overlap)
    } yield edge
  }
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

  def deduplicate(data: Iterable[String], k: Int) = BoundaryBucket.removeKmers(centreData, data, k)

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

  def unified(k: Int): Iterable[BoundaryBucket] = {
    if (safeSurrounding.isEmpty && !receiver) {
      Seq()
    } else if (!receiver) {
      surrounding.map(_._1)
    } else {
      Seq(BoundaryBucket(unifiedBucketId, unifiedCore, unifiedBoundary.toArray))
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

  def sum(x: Long, y: Long) = x + y
}