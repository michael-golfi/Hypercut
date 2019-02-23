package dbpart.ubucket
import scala.collection.JavaConverters._
import kyotocabinet._
import friedrich.util.Distribution
import friedrich.util.Histogram
import scala.collection.mutable.ArrayBuffer
import dbpart.FastQ
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
import scala.annotation.tailrec

trait KyotoDB {
  def dbLocation: String
  def dbOptions: String

  val db = new DB()
  if (!db.open(s"$dbLocation$dbOptions", DB.OWRITER | DB.OCREATE)) {
    throw new Exception("Unable to open db")
  }
  println(s"$dbLocation open")

  addHook()


    /**
   * Ensure that we close the database on JVM shutdown
   */
  def addHook() {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() {
        println(s"Close $dbLocation")
        db.close()
      }
    })
  }
}

/**
 * Database of unique buckets for k-mers.
 * k-mers with identical sequence data are stored together.
 */
abstract class BucketDB[B <: Bucket[B]](val dbLocation: String, val dbOptions: String,
    val unpacker: Unpacker[B],
    k: Int) extends KyotoDB {

  def newBucket(values: Iterable[String]): B

  def unpack(key: String, bucket: String) = unpacker.unpack(key, bucket, k)

  def addSingle(key: String, record: String) {
    val v = Option(db.get(key))
    v match {
      case Some(value) =>
        if (value.size > 2000) {
          println(s"Key $key size ${value.size}")
        }
        unpack(key, value).insertSingle(record) match {
          case Some(nv) => db.set(key, nv.pack)
          case None =>
        }
      case None =>
        db.set(key, newBucket(List(record)).pack)
    }
  }

  /**
   * Copy all values from another database of the same type.
   */
  def copyAllFrom(other: BucketDB[B]) {
    for (bs <- other.buckets.grouped(1000)) {
      val data = Map() ++ bs.map(b => b._1 -> b._2)
      val dataPk = Map() ++ bs.map(b => b._1 -> b._2.pack)
      db.set_bulk(dataPk.asJava, false)
      afterBulkWrite(data)
    }
  }

  /**
   * Merge new values into the existing values. Returns a map of buckets
   * that need to be written back to the database.
   */
  def merge(oldVals: CMap[String, B], from: CMap[String, Iterable[String]]) = {
    var r = MMap[String, B]()
    for ((k, vs) <- from) {
      oldVals.get(k) match {
        case Some(existingBucket) =>
          existingBucket.insertBulk(vs) match {
            case Some(ins) =>
              r += k -> ins
            case None =>
          }
        case None =>
          val ins = newBucket(vs)
          r += k -> ins
      }
    }
    r
  }

  protected def shouldWriteBack(key: String, bucket: B): Boolean = true

  protected def afterBulkWrite(merged: CMap[String, B]) {}

  protected def beforeBulkLoad(keys: Iterable[String]) {}

  class InsertStats {
    var writeback = 0
    var total = 0
    def add(wb: Int, tot: Int) = synchronized {
      writeback += wb
      total += tot
    }
    def print() = synchronized {
      if (total > 0) {
        println(s"Write back ${writeback * 100 / total}% of buckets ($writeback)")
      }
    }
  }

  /**
   * Add pairs of buckets and sequences.
   */
  def addBulk(data: Iterable[(String, String)]) {
    val insert = data.groupBy(_._1).mapValues(vs => vs.map(_._2))
//    Distribution.printStats("Insertion buckets", insert.map(_._2.size))

    val stats = new InsertStats
    for (insertGr <- insert.grouped(10000).toSeq.par) {
      val existing = getBulk(insertGr.keys)
      val merged = merge(existing, insertGr)
      afterBulkWrite(merged)
      val forWrite = merged.filter(x => shouldWriteBack(x._1, x._2)).map(
        x => (x._1 -> x._2.pack))

      stats.add(forWrite.size, merged.size)
      db.set_bulk(forWrite.asJava, false)
    }
    stats.print()
  }

  def getBulk(keys: Iterable[String]): CMap[String, B] = synchronized {
    beforeBulkLoad(keys)
    db.get_bulk(seqAsJavaList(keys.toSeq), false).asScala.map(x => (x._1 -> unpack(x._1, x._2)))
  }

  private def bucketsRaw: Iterator[(String, String)] = {
    new Iterator[(String, String)] {
      val cur = db.cursor()
      cur.jump()
      var nextVal: Array[String] = cur.get_str(true)

      override def next() = {
        val r = (nextVal(0), nextVal(1))
        nextVal = cur.get_str(true)
        r
      }

      override def hasNext() = {
        if (nextVal != null) {
          true
        } else {
          cur.disable()
          false
        }
      }
    }
  }

  def bucketKeys: Iterator[String] =
    bucketsRaw.map(_._1)

  def buckets: Iterator[(String, B)] =
    bucketsRaw.map(x => (x._1, unpack(x._1, x._2)))

  def bucketSizeStats() = {
    val r = new Distribution
    for ((b, vs) <- buckets) {
      r.observe(vs.size)
    }
    r
  }

  def bucketSizeHistogram() = {
    val ss = buckets.map(_._2.size)
    new Histogram(ss.toSeq)
  }

}

final class EdgeDB(location: String, options: String)
  extends BucketDB[DistinctBucket](location, options, DistinctBucket, 0) {

  def newBucket(values: Iterable[String]) = {
    val seed = values.head
    val b = new DistinctBucket(seed, List(seed))
    b.insertBulk(values.tail).getOrElse(b)
  }

  def allEdges: Iterator[(String, String)] = buckets.flatMap(b => b._2.items.map(to => (b._1, to)))

  def allEdges[A](f: String => A): Iterator[(A, A)] =
    buckets.flatMap(b => b._2.items.map(to =>
      (f(b._1), f(to))
    ))
}

/**
 * BucketDB that merges k-mers into contiguous paths.
 * Buckets will contain lists of paths, not KMers.
 * To obtain individual KMers, methods such as kmerBuckets and kmerHistogram
 * can be used.
 *
 * The coverage filter, if present, affects extractor methods such as kmerBuckets, buckets,
 * getBulk, bucketKeys.
 */
final class SeqBucketDB(location: String, options: String, val k: Int, minCoverage: Option[Int])
extends BucketDB[CountingSeqBucket](location, options,
    new CountingUnpacker(location, minCoverage), k) {

  def covDB = unpacker.asInstanceOf[CountingUnpacker].covDB

  def newBucket(values: Iterable[String]) =
    new CountingSeqBucket(Iterable.empty, new CoverageBucket(Iterable.empty), k).insertBulk(values).get

  override def buckets =
    super.buckets.filter(! _._2.sequences.isEmpty)

  //Note: this is less efficient than the supertype operation since we have to unpack every bucket
  //and filter by coverage
  override def bucketKeys =
    buckets.map(_._1)

  /**
   * Only write back sequences if they did actually change
   */
  override protected def shouldWriteBack(key: String, bucket: CountingSeqBucket): Boolean =
    bucket.sequencesUpdated

  override protected def beforeBulkLoad(keys: Iterable[String]) {
    covDB.bulkLoad(keys)
  }

  /**
   * Always write back coverage when a bucket changes
   */
  override protected def afterBulkWrite(merged: CMap[String, CountingSeqBucket]) {
    covDB.setBulk(merged.map(x => x._1 -> x._2.coverage))
  }

  def kmerBuckets = {
    buckets.map((kv) => (kv._1, kv._2.sequences.flatMap(s => {
      s.sliding(k)
    })))
  }

  def kmerCoverageStats = {
    val d = new Distribution
    for ((k, b) <- buckets) {
      d.observe(b.kmerCoverages)
    }
    d
  }

  def kmerStats = {
    val r = new Distribution
    for ((b, vs) <- kmerBuckets) {
      r.observe(vs.size)
    }
    r
  }

  def kmerHistogram = {
    val ss = kmerBuckets.map(_._2.size)
    new Histogram(ss.toSeq)
  }

}

object BucketDB {

  val c1g = 1L * 1204 * 1204 * 1024
  val c4g = 4 * c1g
  val c8g = 8 * c1g
  val c20g = 20L * c1g
  val c40g = 40L * c1g

  val buckets = 20000000
  //20M buckets
  //256 byte alignment
  //40G mmap
  val options = s"#bnum=$buckets#apow=8#opts=l"
  val mmapOptions = s"$options#msiz=$c40g"

}