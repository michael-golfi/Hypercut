package dbpart.bucketdb
import scala.collection.JavaConverters._
import kyotocabinet._
import friedrich.util.Distribution
import friedrich.util.Histogram
import scala.collection.mutable.ArrayBuffer
import dbpart.ReadFiles
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
import scala.annotation.tailrec
import scala.concurrent.blocking

/**
 * A Kyoto cabinet-based database.
 */
trait KyotoDB {
  def dbLocation: String
  def dbOptions: String

  @volatile
  var shouldQuit = false

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
        shouldQuit = true
        println(s"Close $dbLocation")
        db.close()
      }
    })
  }

  def setBulk(kvs: Iterable[(String, String)]) {
    val recary = Array.ofDim[Array[Byte]](kvs.size * 2)
    var ridx = 0
    for (kv <- kvs) {
      recary(ridx) = kv._1.getBytes
      ridx += 1
      recary(ridx) = kv._2.getBytes
      ridx += 1
    }
    db.set_bulk(recary, false)
  }

  protected def bucketsRaw: Vector[(String, String)] = {
    var r = Vector[(String, String)]()
    visitReadonly((key, value) => r :+= (key, value))
    r
  }

  def readonlyVisitor(f: (String, String) => Unit) = new Visitor {
    def visit_full(key: Array[Byte], value: Array[Byte]) = {
      if (!shouldQuit) {
        f(new String(key), new String(value))
      }
      Visitor.NOP
    }

    def visit_empty(key: Array[Byte]) = Visitor.NOP
  }

  def visitReadonly(f: (String, String) => Unit) {
     db.iterate(readonlyVisitor(f), false)
  }

  def visitReadonly(keys: Iterable[String], f: (String, String) => Unit) {
    val keyArray = Array.ofDim[Array[Byte]](keys.size)
    var ridx = 0
    for (key <- keys) {
      keyArray(ridx) = key.getBytes
      ridx += 1
    }
    db.accept_bulk(keyArray, readonlyVisitor(f), false)
  }
}

trait UnpackingDB[B] extends KyotoDB {
  def unpack(key: String, bucket: String): B

  def visitBucketsReadonly(f: (String, B) => Unit) {
    visitReadonly((key, value) => {
      f(key, unpack(key, value))
    })
  }

  def visitBucketsReadonly(keys: Iterable[String], f: (String, B) => Unit) {
    visitReadonly(keys, (key, value) => f(key, unpack(key, value)))
  }
}

/**
 * A database that stores keyed buckets of some type B, which can be serialised
 * to strings. Efficient bulk insertion is supported.
 */
abstract class BucketDB[B <: Bucket[B]](val dbLocation: String, val dbOptions: String,
    val unpacker: Unpacker[B],
    k: Int) extends UnpackingDB[B] {

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
   * Merge new values into the existing values. Returns a list of buckets
   * that need to be written back to the database.
   */
  def merge(oldVals: CMap[String, B], from: CMap[String, Iterable[String]]) = {
    var r = List[(String, B)]()
    for ((k, vs) <- from) {
      oldVals.get(k) match {
        case Some(existingBucket) =>
          existingBucket.insertBulk(vs) match {
            case Some(ins) =>
              r ::= (k, ins)
            case None =>
          }
        case None =>
          val ins = newBucket(vs)
          r ::= (k, ins)
      }
    }
    r
  }

  protected def shouldWriteBack(key: String, bucket: B): Boolean = true

  protected def afterBulkWrite(merged: Iterable[(String, B)]) {}

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
    addBulk(insert)
  }

  def addBulk(insert: CMap[String, Iterable[String]]) {
    val stats = new InsertStats
    for (insertGr <- insert.grouped(10000).toSeq.par) {
      val existing = getBulk(insertGr.keys)
      val merged = merge(existing, insertGr)
      afterBulkWrite(merged)
      val forWrite = merged.filter(x => shouldWriteBack(x._1, x._2)).map(
        x => (x._1 -> x._2.pack))

      stats.add(forWrite.size, merged.size)
      setBulk(forWrite)
    }
    stats.print()
  }


  def getBulk(keys: Iterable[String]): CMap[String, B] = synchronized {
    beforeBulkLoad(keys)
    var r = MMap[String,B]()
    visitBucketsReadonly(keys, (key, bucket) => { r += key -> bucket })
    r
  }

  def buckets: Vector[(String, B)] =
    bucketsRaw.map(x => (x._1, unpack(x._1, x._2)))

  def bucketSizeStats() = {
    val r = new Distribution
    visitBucketsReadonly((key, value) => r.observe(value.size))
    r
  }

  def bucketSizeHistogram(limitMax: Option[Long] = None) = {
    val ss = buckets.map(_._2.size)
    new Histogram(ss, 10, limitMax)
  }
}

object EdgeDB {
  import SeqBucketDB._

  /**
   * 128 byte alignment
   * 8 GB mmap
   */
  def dbOptions: String = s"#bnum=$buckets#apow=7#mmap=$c8g#opts=l"
}

/**
 * Database of nodes and edges in the macro graph. Buckets here simply store
 * unique strings.
 */
final class EdgeDB(location: String)
  extends BucketDB[DistinctBucket](location, EdgeDB.dbOptions, DistinctBucket, 0) {

  def newBucket(values: Iterable[String]) = {
    val seed = values.head
    val b = new DistinctBucket(seed, List(seed))
    b.insertBulk(values.tail).getOrElse(b)
  }

  def allEdges: Iterator[(String, String)] =
    buckets.iterator.flatMap(b => b._2.items.map(to => (b._1, to)))

  def allEdges[A](f: String => A): Iterator[(A, A)] =
    buckets.iterator.flatMap(b => b._2.items.map(to =>
      (f(b._1), f(to))
    ))

  def visitEdges(f: List[(String, String)] => Unit) {
    visitBucketsReadonly((key, value) => {
      f(value.items.map(to => (key, to)))
    })
  }
}

object SeqBucketDB {
  val c1g = 1L * 1204 * 1204 * 1024
  val c4g = 4 * c1g
  val c8g = 8 * c1g
  val c20g = 20L * c1g
  val c40g = 40L * c1g

  val buckets = 40000000
  //40M buckets
  //256 byte alignment
  //40G mmap
  val options = s"#bnum=$buckets#apow=8#opts=l"
  val mmapOptions = s"$options#msiz=$c40g"
}

/**
 * BucketDB that merges k-mers into contiguous paths.
 * Buckets will contain lists of paths, not KMers.
 * To obtain individual KMers, methods such as kmerBuckets and kmerHistogram
 * can be used.
 *
 * The coverage of each k-mer is also tracked in an auxiliary database (covDB).
 *
 * The coverage filter, if present, affects extractor methods such as kmerBuckets, buckets,
 * getBulk, bucketKeys.
 */
final class SeqBucketDB(location: String, options: String, val k: Int, minCoverage: Option[Int])
extends BucketDB[CountingSeqBucket](location, options,
    new CountingUnpacker(location, minCoverage), k) {

  def covDB = unpacker.asInstanceOf[CountingUnpacker].covDB

  //Traversing the coverages should be cheaper than traversing the full buckets
  //for counting the number of sequences
  override def bucketSizeHistogram(limitMax: Option[Long] = None) = {
    val ss = covDB.buckets.map(_._2.coverages.size)
    new Histogram(ss.toSeq, 10, limitMax)
  }

  def newBucket(values: Iterable[String]) =
    new CountingSeqBucket(Iterable.empty, new CoverageBucket(Iterable.empty), k).insertBulk(values).get

  override def buckets =
    super.buckets.filter(! _._2.sequences.isEmpty)

  /*
   * Going purely through the coverage DB is cheaper than unpacking all sequences
   */
  def visitKeysReadonly(f: (String) => Unit) {
    minCoverage match {
      case Some(m) =>
        covDB.visitBucketsReadonly((key, b) => {
          if (b.hasMinCoverage(m)) {
            f(key)
          }
        })
      case None =>
        covDB.visitBucketsReadonly((key, b) => f(key))
    }
  }

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
  override protected def afterBulkWrite(merged: Iterable[(String, CountingSeqBucket)]) {
    covDB.setBulk(merged.map(x => (x._1 -> x._2.coverage.pack)))
  }

  def kmerCoverageStats = {
    val d = new Distribution
    covDB.visitBucketsReadonly((key, b) => {
      d.observe(b.kmerCoverages)
    })
    d
  }

  def kmerStats = {
    val r = new Distribution
    covDB.visitBucketsReadonly((key, bucket) => {
      r.observe(bucket.coverages.map(_.length).sum)
    })
    r
  }
}
