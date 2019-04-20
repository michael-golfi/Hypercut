package dbpart.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import miniasm.genome.util.DNAHelpers
import dbpart.hash._
import dbpart._

/**
 * Helper routines for executing Hypercut from Apache Spark.
 */
class Routines(spark: SparkSession) {
  implicit val sc = spark.sparkContext
  import spark.sqlContext.implicits._

  /**
   * Load reads and their reverse complements from DNA files.
   */
  def getReads(fileSpec: String): Dataset[String] = {
    val reads = sc.textFile(fileSpec).toDF.map(_.getString(0))
    val withRev = reads.flatMap(r => Seq(r, DNAHelpers.reverseComplement(r)))
    withRev
  }

  def hashReads(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, String)] = {
    reads.flatMap(r => ext.compactMarkers(r))
  }

  def edges(reads: Dataset[String], ext: MarkerSetExtractor): Dataset[(CompactNode, CompactNode)] = {
   val mss = reads.map(r => ext.markerSetsInRead(r))
   mss.flatMap(readMss => {
     var r = List[(CompactNode, CompactNode)]()
     MarkerSetExtractor.visitTransitions(readMss, (a,b) => r ::= (a.compact,b.compact))
     r
   }).distinct
  }
}