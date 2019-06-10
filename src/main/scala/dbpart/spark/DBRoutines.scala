package dbpart.spark

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import dbpart.hash.MarkerSetExtractor
import dbpart.Settings
import dbpart.bucketdb._
import dbpart.bucket._
import dbpart.hash.MarkerSet

/**
 * Routines for writing spark-generated data into bucket databases.
 */
class DBRoutines(spark: SparkSession) extends Routines(spark) {
  import spark.sqlContext.implicits._

  def writeEdges(edges: Dataset[(Array[Byte], Array[Byte])], ext: MarkerSetExtractor, dbFile: String) {
    val d = edges.cache
    val data = d.groupByKey(_._1)
    val n = data.keys.count
    val set = Settings.settings(dbFile, n.toInt)
    val db = set.edgeDb(ext.space)
    val it = data.mapGroups((g, vs) => (g, vs.map(_._2).toList)).toLocalIterator()
    for {
      g <- it.asScala.grouped(1000000)
      m = g.map(x => (x._1, db.newBucket(x._2)))
    } {
      db.overwriteBulk(m)
    }
    db.close
    d.unpersist
  }

  def writeBuckets(buckets: Dataset[(Array[Byte], SimpleCountingBucket)], ext: MarkerSetExtractor, dbFile: String) {
    val data = buckets.cache
    val n = data.count
    val set = Settings.settings(dbFile, n.toInt)
    val db = set.writeBucketDb(ext.k)
    val it = data.toLocalIterator
     for {
      g <- it.asScala.grouped(1000000)
      mg = g.map(x =>
        (MarkerSet.uncompactToString(x._1, ext.space) ->
        PackedSeqBucket(x._2.sequences, x._2.abundances, x._2.k)))
      m = mg.toMap
    } {
      db.overwriteBulk(m)
    }
    db.close
    data.unpersist
  }
//
//  def writeEdgesAndBuckets(reads: Dataset[String], ext: MarkerSetExtractor, dbFile: String) {
//    val segments = splitReads(reads, ext)
//    writeEdges(edgesFromSplit(segments), ext, dbFile)
//    writeBuckets(hashToBuckets(segments, ext), ext, dbFile)
//  }

}