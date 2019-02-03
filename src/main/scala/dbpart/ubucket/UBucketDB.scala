package dbpart.ubucket
import scala.collection.JavaConverters._
import kyotocabinet._
import friedrich.util.Distribution
import friedrich.util.Histogram
import scala.collection.mutable.ArrayBuffer
import dbpart.FastQ

/**
 * Database of unique buckets for k-mers.
 * k-mers with identical sequence data are stored together.
 */
abstract class BucketDB[B <: Bucket[B]](location: String, options: String, unpacker: Unpacker[B],
    k: Int) {

  /**
   * Ensure that we close the database on JVM shutdown
   */
  def addHook() {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() {
        println(s"Close $location")
        db.close()
      }
    })
  }

  val db = new DB()
  if (!db.open(s"$location$options", DB.OWRITER | DB.OCREATE)) {
    throw new Exception("Unable to open db")
  }
  println(s"$location open")

  addHook()

  def newBucket(values: Iterable[String]): B

  def unpack(bucket: String): B = unpacker.unpack(bucket, k)

  def addSingle(key: String, record: String) {
    val v = Option(db.get(key))
    v match {
      case Some(value) =>
        if (value.size > 2000) {
          println(s"Key $key size ${value.size}")
        }
        unpack(value).insertSingle(record) match {
          case Some(nv) => db.set(key, nv.pack)
          case None =>
        }
      case None =>
        db.set(key, newBucket(List(record)).pack)
    }
  }

  import scala.collection.{Map => CMap}
  import scala.collection.mutable.{Map => MMap}

  /**
   * Merge new values into the existing values. Returns a map of buckets
   * that need to be written back to the database.
   */
  def merge(oldVals: MMap[String, String], from: CMap[String, Iterable[String]]) = {
    var r = MMap[String, String]()
    for ((k, vs) <- from) {
      oldVals.get(k) match {
        case Some(existingBucket) =>
          unpack(existingBucket).insertBulk(vs) match {
            case Some(ins) =>
              r += k -> ins.pack
            case None =>
          }
        case None =>
          r += k -> newBucket(vs).pack
      }
    }
    r
  }

  def addBulk(data: Iterable[(String, String)]) {
    val insert = data.groupBy(_._1).mapValues(vs => vs.map(_._2))
//    Distribution.printStats("Insertion buckets", insert.map(_._2.size))

    for (insertGr <- insert.grouped(1000).toSeq.par) {
      val existing = db.get_bulk(seqAsJavaList(insertGr.keys.toSeq), false)
      //    val existing = MMap[String, String]()
      val merged = merge(existing.asScala, insertGr)
      db.set_bulk(merged.asJava, false)
    }
  }

  def getBulk(keys: Iterable[String]): CMap[String, B] = {
    db.get_bulk(seqAsJavaList(keys.toSeq), false).asScala.map(x => (x._1 -> unpack(x._2)))
  }

  private def bucketsRaw: Iterator[(String, String)] = {
    new Iterator[(String, String)] {
      val cur = db.cursor()
      cur.jump()
      var continue = cur.step()
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
    bucketsRaw.map(x => (x._1, unpack(x._2)))

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

/**
 * BucketDB that merges k-mers into contiguous paths.
 * Buckets will contain lists of paths, not KMers.
 * To obtain individual KMers, methods such as kmerBuckets and kmerHistogram
 * can be used.
 */
final class SeqBucketDB(location: String, options: String, val k: Int)
extends BucketDB[SeqBucket](location, options, SeqBucket, k) {

  def newBucket(values: Iterable[String]) =
    new SeqBucket(Seq(), k).insertBulk(values).get

  def kmerBuckets = {
    buckets.map((kv) => (kv._1, kv._2.sequences.flatMap(s => {
      s.sliding(k)
    })))
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

  //40M buckets
  //256 byte alignment
  //20G mmap
  //#comp=zlib
  val options = s"#bnum=40000000#apow=8"
  val mmapOptions = s"$options#msiz=$c20g"

}