package hypercut.bucketdb

import hypercut.hash.MarkerSet
import hypercut.hash.MarkerSpace

object Settings {
  //NB the ideal edge flush interval should be set considering the number of expected edges per node
  //(since the memory cost of buffering edges can be high)

  def noindexSettings(dbfile: String, buckets: Int): Settings =
    new Settings(dbfile, buckets, Some(20000000), 50000)

  def settings(dbfile: String, buckets: Int): Settings =
    new Settings(dbfile, buckets, Some(5000000), 20000)
}

/**
 * Performance and I/O related settings for the application.
 */
class Settings(val dbfile: String,
  val buckets: Int,
  val edgeWriteInterval: Option[Int],
  val readBufferSize: Int) {

  def edgeDb(space: MarkerSpace): EdgeDB = {
    val compact = MarkerSet.compactSize(space)
    new EdgeDB(dbfile.replace(".kch", "_edge.kch"), buckets,
      new DistinctByteBucket.Unpacker(compact))
  }

  def writeBucketDb(k: Int) = {
    val opts = SeqBucketDB.mmapOptions(buckets)
    new SeqBucketDB(dbfile, opts, buckets, k, None)
  }
}