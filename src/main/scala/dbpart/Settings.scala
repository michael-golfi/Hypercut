package dbpart

import dbpart.bucketdb.EdgeDB

object Settings {
  def noindexSettings(dbfile: String, buckets: Int): dbpart.Settings =
    new Settings(dbfile, buckets, Some(7500000), 50000)

  def settings(dbfile: String, buckets: Int): dbpart.Settings =
    new Settings(dbfile, buckets, Some(5000000), 20000)
}

/**
 * Performance and I/O related settings for the application.
 */
class Settings(val dbfile: String,
  val buckets: Int,
  val edgeWriteInterval: Option[Int],
  val readBufferSize: Int) {

  def edgeDb: dbpart.bucketdb.EdgeDB = new EdgeDB(dbfile.replace(".kch", "_edge.kch"), buckets)
}