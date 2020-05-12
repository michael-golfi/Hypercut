package hypercut.spark

import java.io.{File, PrintStream}

import hypercut._
import hypercut.bucket._
import hypercut.hash.{FeatureScanner, _}
import friedrich.util._
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer._
import miniasm.genome.util.DNAHelpers
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import vegas._

final case class HashSegment(hash: Array[Byte], segment: ZeroBPBuffer)

final case class CountedHashSegment(hash: Array[Byte], segment: ZeroBPBuffer, count: Long)

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {

  import SerialRoutines._

  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import hypercut.bucket.CountingSeqBucket._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  type Kmer = String
  type Segment = String

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReadsFromFasta(fileSpec: String, withRC: Boolean, frac: Option[Double] = None): Dataset[String] = {
    //TODO: add boolean flag to include/not include RCs
    val lines = frac match {
      case Some(f) => sc.textFile(fileSpec).toDS.sample(f)
      case None => sc.textFile(fileSpec).toDS
    }

    if (withRC) {
      lines.flatMap(r => {
        if (r.startsWith(">")) Seq() else
          Seq(r, DNAHelpers.reverseComplement(r))
      })
    } else {
      lines.filter(r => !r.startsWith(">"))
    }
  }

  /**
   * Count motifs such as AC, AT, TTT in a set of reads.
   */
  def countFeatures(reads: Dataset[String], space: MotifSpace): FeatureCounter = {
    reads.map(r => {
      val c = new FeatureCounter
      val s = new FeatureScanner(space)
      s.scanRead(c, r)
      c
    }).reduce(_ + _)
  }

  def createSampledSpace(input: String, fraction: Double, space: MotifSpace): MotifSpace = {
    val in = getReadsFromFasta(input, true, Some(fraction))
    val counter = countFeatures(in, space)
    counter.print(s"Discovered frequencies in fraction $fraction")
    counter.toSpaceByFrequency(space.n)
  }

  /**
   * The motif sets in a read, paired with the corresponding segments that were discovered.
   */

  type ProcessedRead = (Array[HashSegment])

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
  def bucketsOnly[H](reads: Dataset[String], spl: ReadSplitter[H]): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val segments = reads.flatMap(r => createHashSegments(r, spl))
    processedToBuckets(segments, spl)
  }

  /**
   * Convenience function to compute buckets directly from an input specification
   * and optionally write them to the output location.
   */
  def bucketsOnly[H](input: String, spl: ReadSplitter[H], output: Option[String]): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val reads = getReadsFromFasta(input, false)
    val bkts = bucketsOnly(reads, spl)
    for (o <- output) {
      writeBuckets(bkts, o)
    }
    bkts
  }

  def processedToBuckets[H](segments: Dataset[HashSegment], spl: ReadSplitter[H],
                            reduce: Boolean = false): Dataset[(Array[Byte], SimpleCountingBucket)] = {
    val countedSegments =
      segments.groupBy($"hash", $"segment").count.
        as[CountedHashSegment].cache

    val reverseSegments = countedSegments.flatMap(x => {
      val s = x.segment.toString
      val rc = DNAHelpers.reverseComplement(s)
      val revSegments = createHashSegments(rc, spl)
      revSegments.map(s => CountedHashSegment(s.hash, s.segment, x.count))
    })

    val grouped = countedSegments.union(reverseSegments).groupBy($"hash")

    val collected = grouped.agg(collect_list(struct($"segment", $"count"))).
      as[(Array[Byte], Array[(ZeroBPBuffer, Long)])]

    collected.map { case (hash, segmentsCounts) => {
      val empty = SimpleCountingBucket.empty(spl.k)
      val bkt = empty.insertBulkSegments(segmentsCounts.map(x =>
        (x._1.toString, clipAbundance(x._2))).toList)
      (hash, bkt)
    }
    }
  }

  def log10(x: Double) = Math.log(x) / Math.log(10)

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
                  writeLocation: Option[String] = None): GraphFrame = {
    //NB this assumes we can get all significant edges without looking at reverse complements
    val edges = splitReadsToEdges(getReadsFromFasta(reads, false), ext).distinct
    val verts = bucketsOnly(getReadsFromFasta(reads, false), ext)

    edges.cache
    verts.cache
    for (output <- writeLocation) {
      edges.write.mode(SaveMode.Overwrite).parquet(s"${output}_edges")
      writeBuckets(verts, output)
    }

    toGraphFrame(verts, edges)
  }

  def toGraphFrame(
                    bkts: Dataset[(Array[Byte], SimpleCountingBucket)],
                    edges: Dataset[(CompactEdge)]): GraphFrame = {
    val vertDF = bkts.toDF("id", "bucket")
    val edgeDF = edges.toDF("src", "dst")

    GraphFrame(vertDF, edgeDF)
  }

  def writeBuckets(bkts: Dataset[(Array[Byte], SimpleCountingBucket)], writeLocation: String) {
    bkts.write.mode(SaveMode.Overwrite).parquet(s"${writeLocation}_buckets")
  }

  def loadBuckets(readLocation: String, minAbundance: Option[Abundance]) = {
    val buckets = spark.read.parquet(s"${readLocation}_buckets").as[(Array[Byte], SimpleCountingBucket)]
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
                   bkts: Dataset[(Array[Byte], SimpleCountingBucket)],
                   edges: Option[Dataset[CompactEdge]],
                   output: PrintStream) {

    def sumLongs(ds: Dataset[Long]) = ds.reduce(_ + _)

    val stats = bkts.map(_._2.stats).cache

    Console.withOut(output) {
      println("Sequence count in buckets: sum " + sumLongs(stats.map(_.sequences.toLong)))
      println("Kmer count in buckets: sum " + sumLongs(stats.map(_.kmers.toLong)))
      println("k-mer abundance: sum " + sumLongs(stats.map(_.totalAbundance.toLong)))
      println("Bucket stats:")
      stats.describe().show()
      stats.unpersist

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

    val r = bucketGraph(input, ext, outputLocation)
    r
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

/**
 * Serialization-safe routines.
 */
object SerialRoutines {
  def removeN(segment: String, k: Int): Iterator[String] = {
    segment.split("N", -1).iterator.filter(s => s.length() >= k)
  }

  def lengthFilter(minLength: Option[Int])(c: Contig) = minLength match {
    case Some(ml) => if (c.length >= ml) Some(c) else None
    case _ => Some(c)
  }

  def createHashSegments[H](r: String, spl: ReadSplitter[H]) = {
    for {
      (h, s) <- spl.split(r)
      ss <- removeN(s, spl.k)
      r = HashSegment(spl.compact(h), BPBuffer.wrap(ss))
    } yield r
  }
}
