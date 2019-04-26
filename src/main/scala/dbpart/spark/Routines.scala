package dbpart.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import dbpart._
import dbpart.bucketdb._
import dbpart.hash._
import miniasm.genome.util.DNAHelpers

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  implicit val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String): Dataset[String] = {
    val reads = sc.textFile(fileSpec).toDF.map(_.getString(0))
    val withRev = reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
    withRev
  }

  def countFeatures(reads: Dataset[String], space: MarkerSpace) = {
    reads.map(r => {
      val c = new FeatureCounter
      val s = new FeatureScanner(space)
      s.scanRead(c, r)
      c
    }).reduce( _+_ )
  }

  def hashReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, String)] = {
    reads.flatMap(r => ext.compactMarkers(r))
  }

  def splitReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[Array[(CompactNode, String)]] = {
    reads.map(r => {
      val buckets = ext.markerSetsInRead(r)._2
      ext.splitRead(r, buckets).iterator.map(x => (x._1.compact, x._2)).toArray
    })
  }

  def hashToBuckets(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, CountingSeqBucket)] = {
    reads.flatMap(r => {
      val ms = ext.compactMarkers(r)
      val grouped = MarkerSetExtractor.groupTransitions(ms)
      grouped.map(g => (g._1, CountingSeqBucket.newBucket(g._2, ext.k)))
    })
  }

  def edges(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, CompactNode)] = {
   val mss = reads.map(r => ext.markerSetsInRead(r))
   mss.flatMap(readMss => {
     var r = List[(CompactNode, CompactNode)]()
     MarkerSetExtractor.visitTransitions(readMss._1, (a,b) => r ::= (a.compact,b.compact))
     r
   }).distinct
  }

  def buckets(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, CountingSeqBucket)] = {
    val hashed = hashToBuckets(reads, ext)
    val grouped = hashed.groupByKey(_._1).mapValues(_._2)
    grouped.reduceGroups(_ mergeBucket _)
  }
}