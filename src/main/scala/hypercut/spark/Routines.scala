package hypercut.spark

import java.io.{File, PrintStream}

import hypercut.hash.{FeatureScanner, _}
import hypercut.bucket.BucketStats
import hypercut.graph.Contig
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer._
import miniasm.genome.util.DNAHelpers
import org.apache.spark.sql.SparkSession

final case class HashSegment(hash: Array[Byte], segment: ZeroBPBuffer)

final case class CountedHashSegment(hash: Array[Byte], segment: ZeroBPBuffer, count: Long)

/**
 * Core routines for executing Hypercut from Apache Spark.
 */
class Routines(val spark: SparkSession) {

  import SerialRoutines._

  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._


  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReadsFromFasta(fileSpec: String, withRC: Boolean, frac: Option[Double] = None): Dataset[String] = {
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


  def countedSegmentsByHash[H](segments: Dataset[HashSegment], spl: ReadSplitter[H],
                               addReverseComplements: Boolean) = {
    val countedSegments = if (addReverseComplements) {
      val step1 =
        segments.groupBy($"hash", $"segment").count.
          as[CountedHashSegment].cache

      val reverseSegments = step1.flatMap(x => {
        val s = x.segment.toString
        val rc = DNAHelpers.reverseComplement(s)
        val revSegments = createHashSegments(rc, spl)
        revSegments.map(s => CountedHashSegment(s.hash, s.segment, x.count))
      })
      step1 union reverseSegments
    } else {
      //NB not caching in this case
      segments.groupBy($"hash", $"segment").count.
        as[CountedHashSegment]
    }

    val grouped = countedSegments.groupBy($"hash")
    grouped.agg(collect_list(struct($"segment", $"count"))).
      as[(Array[Byte], Array[(ZeroBPBuffer, Long)])]
  }

  def segmentsByHash[H](segments: Dataset[HashSegment], spl: ReadSplitter[H],
                        addReverseComplements: Boolean) = {
    assert(!addReverseComplements) //not yet implemented

    val grouped = segments.groupBy($"hash")
    grouped.agg(collect_list($"segment")).
      as[(Array[Byte], Array[ZeroBPBuffer])]
  }


  def showStats(stats: Dataset[BucketStats], output: PrintStream): Unit = {
    def sumLongs(ds: Dataset[Long]) = ds.reduce(_ + _)

    stats.cache
    println("Sequence count in buckets: sum " + sumLongs(stats.map(_.sequences)))
    println("Kmer count in buckets: sum " + sumLongs(stats.map(_.kmers)))
    println("k-mer abundance: sum " + sumLongs(stats.map(_.totalAbundance)))
    println("Bucket stats:")
    stats.describe().show()
    stats.unpersist
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
