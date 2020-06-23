package hypercut.spark

import java.io.PrintStream

import friedrich.util.Histogram
import hypercut.{Abundance, CompactEdge}
import hypercut.bucket.SimpleCountingBucket
import hypercut.hash.{BucketId, MotifSetExtractor, ReadSplitter}
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.graphframes.GraphFrame
import vegas.{Bar, Quant, Vegas}

class BucketGraph(routines: Routines) {
  val spark = routines.spark

  import SerialRoutines._

  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import hypercut.bucket.AbundanceBucket._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  /**
   * Hash and split a set of reads into edges
   */
  def splitReadsToEdges(reads: Dataset[String], ext: MotifSetExtractor): Dataset[(Array[Byte], Array[Byte])] = {
    reads.flatMap(r => {
      val buckets = ext.motifSetsInRead(r)._2
      MotifSetExtractor.collectTransitions(
        buckets.map(x => x._1.compact.data).toList
      )
    })
  }

  /**
   * Simplified version of bucketGraph that only builds buckets
   * (thus, counting k-mers), not collecting edges.
   */
  def bucketsOnly[H](reads: Dataset[String], spl: ReadSplitter[H],
                     addReverseComplements: Boolean): Dataset[(BucketId, SimpleCountingBucket)] = {
    val bcSplit = sc.broadcast(spl) //TODO test
    val segments = reads.flatMap(r => createHashSegments(r, spl))
    countedToSequenceBuckets(
      routines.countedSegmentsByHash(segments, bcSplit, addReverseComplements),
      spl.k
    )
  }

  def countedToSequenceBuckets(counted: Dataset[(BucketId, Array[(ZeroBPBuffer, Long)])], k: Int) =
    counted.map { case (hash, segmentsCounts) => {
      val empty = SimpleCountingBucket.empty(k)
      val bkt = empty.insertBulkSegments(segmentsCounts.map(x =>
        (x._1.toString, clipAbundance(x._2))))
      (hash, bkt)
    } }

  /**
   * Convenience function to compute buckets directly from an input specification
   * and optionally write them to the output location.
   */
  def bucketsOnly[H](input: String, spl: ReadSplitter[H], output: Option[String],
                     addReverseComplements: Boolean): Dataset[(BucketId, SimpleCountingBucket)] = {
    val reads = routines.getReadsFromFiles(input, addReverseComplements)
    val bkts = bucketsOnly(reads, spl, addReverseComplements)
    for (o <- output) {
      writeBuckets(bkts, o)
    }
    bkts
  }

  def log10(x: Double) = Math.log10(x)

  def plotBuckets(location: String, numBins: Int) {
    //Get the buckets
    val buckets = loadBuckets(location, None)
    val hist = new Histogram(buckets.map(_._2.numKmers).collect, numBins)
    val bins = (hist.bins.map(b => "%.1f".format(b)))
    val counts = hist.counts.map(x => log10(x))
    val points = bins zip counts
    //
    //      println(bins)
    hist.print("number of kmers")
    val plot = Vegas("Bucket Histogram", width = 800, height = 600).
      withData(
        points.map(p => Map("bins" -> p._1, "Kmers" -> p._2))).
      encodeX("bins", Quant).
      encodeY("Kmers", Quant).
      mark(Bar).show
  }

  /**
   * Construct a graph where the buckets are vertices, and edges are detected from
   * adjacent segments.
   * Optionally save it in parquet format to a specified location.
   */
  def bucketGraph(reads: String, ext: MotifSetExtractor,
                  writeLocation: Option[String] = None,
                  addReverseComplements: Boolean = true): GraphFrame = {
    //NB this assumes we can get all significant edges without looking at reverse complements
    val edges = splitReadsToEdges(
      routines.getReadsFromFiles(reads, false), ext).distinct
    val verts = bucketsOnly(
      routines.getReadsFromFiles(reads, false), ext, addReverseComplements)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
    }

    toGraphFrame(verts, edges)
  }

  def toGraphFrame(
                    bkts: Dataset[(BucketId, SimpleCountingBucket)],
                    edges: Dataset[(CompactEdge)]): GraphFrame = {
    val vertDF = bkts.toDF("id", "bucket")
    val edgeDF = edges.toDF("src", "dst")

    GraphFrame(vertDF, edgeDF)
  }

  def writeBuckets(bkts: Dataset[(BucketId, SimpleCountingBucket)], writeLocation: String) {
    bkts.write.mode(SaveMode.Overwrite).parquet(s"${writeLocation}_buckets")
  }

  def loadBuckets(readLocation: String, minAbundance: Option[Abundance]) = {
    val buckets = spark.read.parquet(s"${readLocation}_buckets").as[(BucketId, SimpleCountingBucket)]
    minAbundance match {
      case Some(min) => buckets.flatMap(x => (
        x._2.atMinAbundance(minAbundance).nonEmptyOption.map(b => (x._1, b)))
      )
      case None => buckets
    }
  }

  def loadBucketsEdges(readLocation: String, minAbundance: Option[Abundance]) = {
    val edges = spark.read.parquet(s"${readLocation}_edges").as[CompactEdge]
    val bkts = loadBuckets(readLocation, minAbundance)
    (bkts, edges)
  }

  /**
   * Build graph with edges and buckets (as vertices).
   */
  def loadBucketGraph(readLocation: String, minAbundance: Option[Abundance] = None,
                      limit: Option[Int] = None): GraphFrame = {
    val (buckets, edges) = loadBucketsEdges(readLocation, minAbundance)

    limit match {
      case Some(l) => toGraphFrame(buckets.limit(l), edges)
      case _ => toGraphFrame(buckets, edges)
    }
  }


  def bucketStats(
                   bkts: Dataset[(BucketId, SimpleCountingBucket)],
                   edges: Option[Dataset[CompactEdge]],
                   output: PrintStream) {
    val stats = bkts.map(_._2.stats)

    Console.withOut(output) {
      routines.showStats(stats)
      for (e <- edges) {
        println(s"Total number of edges: ${e.count}")
        val outDeg = e.groupByKey(_._1).count().cache
        println("Outdegree: ")
        outDeg.describe().show()
        outDeg.unpersist
        println("Indegree: ")
        val inDeg = e.groupByKey(_._2).count().cache
        inDeg.describe().show()
        inDeg.unpersist
      }
    }
  }

  def bucketStats(input: String, minAbundance: Option[Abundance], stdout: Boolean) {
    val (verts, edges) = loadBucketsEdges(input, minAbundance) match {
      case (b, e) => (b, Some(e))
    }

    if (stdout) {
      bucketStats(verts, edges, Console.out)
    } else {
      val os = new PrintStream(s"${input}_bucketStats.txt")
      try {
        bucketStats(verts, edges, os)
      } finally {
        os.close
      }
    }
  }

  /**
   * Convenience function to build a GraphFrame directly from reads without
   * loading from saved data.
   * The generated data can optionally be saved in the specified location.
   */
  def graphFromReads(input: String, ext: MotifSetExtractor,
                     outputLocation: Option[String] = None) = {

    bucketGraph(input, ext, outputLocation)
  }

  def showBuckets(buckets: Dataset[SimpleCountingBucket], n: Int, amount: Int) {
    val withSize = buckets.map(b => (b.sequences.size, b))
    val sorted = withSize.sort(desc("_1")).take(n)
    for {(size, bucket) <- sorted} {
      println(s"bucket size $size k-mers ${bucket.numKmers}")
      for {s <- bucket.sequences.take(amount)} {
        println(s"  $s")
      }
    }
  }

}
