package dbpart.bucketdb
import dbpart._
import scala.collection.JavaConverters._
import kyotocabinet._
import friedrich.util.Distribution
import friedrich.util.Histogram
import scala.collection.mutable.ArrayBuffer
import dbpart.shortread.ReadFiles
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
import scala.annotation.tailrec
import scala.concurrent.blocking

/**
 * A Kyoto cabinet-based database.
 */
trait KyotoDB[Packed, B] {
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

  def count = db.count

  //Ensure that we close the database on JVM shutdown
  def addHook() {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() {
        close()
      }
    })
  }

  def close() {
    shouldQuit = true
    println(s"Close $dbLocation")
    db.close()
  }

  type Rec = Array[Byte]
  def readonlyVisitor(f: (Rec, Rec) => Unit) = new Visitor {
    def visit_full(key: Rec, value: Rec) = {
      if (!shouldQuit) {
        f(key, value)
      }
      Visitor.NOP
    }

    def visit_empty(key: Rec) = Visitor.NOP
  }

  def unpack(key: Packed, bucket: Packed): B
  def visitReadonly(keys: Iterable[Packed], f: (Packed, Packed) => Unit)
  def visitReadonly(f: (Packed, Packed) => Unit)

  def visitBucketsReadonly(f: (Packed, B) => Unit) {
    visitReadonly((key, value) => {
      f(key, unpack(key, value))
    })
  }

  def visitBucketsReadonly(keys: Iterable[Packed], f: (Packed, B) => Unit) {
    visitReadonly(keys, (key, value) => f(key, unpack(key, value)))
  }

  def dbSet(key: Packed, value: Packed)
  def dbGet(key: Packed): Packed
  def setBulk(kvs: Iterable[(Packed, Packed)])
}

/**
 *  A KyotoDB that stores byte arrays.
 */
trait ByteKyotoDB[B] extends KyotoDB[Array[Byte], B] {
  def visitReadonly(f: (Rec, Rec) => Unit) {
     db.iterate(readonlyVisitor(f), false)
  }

  def visitReadonly(keys: Iterable[Rec], f: (Rec, Rec) => Unit) {
    db.accept_bulk(keys.toArray, readonlyVisitor(f), false)
  }

  def dbSet(key: Rec, value: Rec) = db.set(key, value)
  def dbGet(key: Rec): Rec = db.get(key)
  def setBulk(kvs: Iterable[(Rec, Rec)]) = {
    val recary = Array.ofDim[Array[Byte]](kvs.size * 2)
    var ridx = 0
    for (kv <- kvs) {
      recary(ridx) = kv._1
      ridx += 1
      recary(ridx) = kv._2
      ridx += 1
    }
    db.set_bulk(recary, false)
  }
}

/**
 * A KyotoDB that stores strings.
 */
trait StringKyotoDB[B] extends KyotoDB[String, B] {
  def dbSet(key: String, value: String) = db.set(key, value)

  def dbGet(key: String): String = db.get(key)

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

  def stringVisitor(f: (String, String) => Unit) =
    readonlyVisitor((key, value) =>
       f(new String(key), new String(value)))

  def visitReadonly(f: (String, String) => Unit) {
     db.iterate(stringVisitor(f), false)
  }

  def visitReadonly(keys: Iterable[String], f: (String, String) => Unit) {
    val keyArray = Array.ofDim[Array[Byte]](keys.size)
    var ridx = 0
    for (key <- keys) {
      keyArray(ridx) = key.getBytes
      ridx += 1
    }
    db.accept_bulk(keyArray, stringVisitor(f), false)
  }

  def unpack(key: String, bucket: String): B
}

/**
 * A database that stores keyed buckets of some type B, which can be serialised
 * to strings. Efficient bulk insertion is supported.
 */
abstract class BucketDB[Packed, B <: Bucket[Packed, B]](val dbLocation: String, val dbOptions: String,
    val unpacker: Unpacker[Packed, B],
    k: Int) extends KyotoDB[Packed, B] {

  def newBucket(values: Iterable[Packed]): B

  def unpack(key: Packed, bucket: Packed) = unpacker.unpack(key, bucket, k)

  def addSingle(key: Packed, record: Packed) {
    val v = Option(dbGet(key))
    v match {
      case Some(value) =>
        unpack(key, value).insertSingle(record) match {
          case Some(nv) => dbSet(key, nv.pack)
          case None =>
        }
      case None =>
        dbSet(key, newBucket(List(record)).pack)
    }
  }

  /**
   * Merge new values into the existing values. Returns a list of buckets
   * that need to be written back to the database.
   */
  def merge(oldVals: CMap[Packed, B], from: CMap[Packed, Iterable[Packed]]) = {
    var r = List[(Packed, B)]()
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

  protected def shouldWriteBack(key: Packed, bucket: B): Boolean = true

  protected def afterBulkWrite(merged: Iterable[(Packed, B)]) {}

  protected def beforeBulkLoad(keys: Iterable[Packed]) {}

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
  def addBulk(data: Iterable[(Packed, Packed)]) {
    val insert = data.groupBy(_._1).mapValues(vs => vs.map(_._2))
    addBulk(insert)
  }

  def addBulk(insert: CMap[Packed, Iterable[Packed]]) {
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

  def overwriteBulk(insert: Iterable[(Packed, B)]) {
    val stats = new InsertStats
    afterBulkWrite(insert)
    setBulk(insert.map(x => (x._1, x._2.pack)))
    stats.add(insert.size, insert.size)
    stats.print()
  }

  def getBulk(keys: Iterable[Packed]): CMap[Packed, B] = synchronized {
    beforeBulkLoad(keys)
    var r = MMap[Packed,B]()
    visitBucketsReadonly(keys, (key, bucket) => { r += key -> bucket })
    r
  }

  def bucketSizeStats() = {
    val r = new Distribution
    visitBucketsReadonly((key, value) => r.observe(value.size))
    r
  }

  def bucketSizeHistogram(limitMax: Option[Long] = None) = {
    var sizes = Vector[Int]()
    visitBucketsReadonly((key, bucket) => {
      sizes :+= bucket.size
    })
    new Histogram(sizes, 10, limitMax)
  }
}

abstract class StringBucketDB[B <: Bucket[String, B]](location: String,
    dbOptions: String, unpacker: Unpacker[String, B], k: Int)
  extends BucketDB[String, B](location, dbOptions, unpacker, k) with StringKyotoDB[B]

abstract class ByteBucketDB[B <: Bucket[Array[Byte], B]](location: String,
    dbOptions: String, unpacker: Unpacker[Array[Byte], B], k: Int)
  extends BucketDB[Array[Byte], B](location, dbOptions, unpacker, k) with ByteKyotoDB[B] {

  // Overriding to handle equality
  override def addBulk(data: Iterable[(Rec, Rec)]) {
    val insert = data.groupBy(_._1).mapValues(vs => vs.map(_._2))
    addBulk(insert)
  }

  // Overriding to handle equality
  override def merge(oldVals: CMap[Rec, B], from: CMap[Rec, Iterable[Rec]]) = {
    val oldValsS = Map() ++ oldVals.map(x => (x._1.toSeq -> x._2))
    var r = List[(Rec, B)]()
    for ((k, vs) <- from) {
      oldValsS.get(k.toSeq) match {
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
}

object EdgeDB {
  import SeqBucketDB._

  /**
   * 32 byte alignment
   * 8 GB mmap
   */
  def dbOptions(buckets: Int): String = s"#bnum=$buckets#apow=5#mmap=$c20g"
}

/**
 * Database of nodes and edges in the macro graph. Buckets here simply store
 * unique strings.
 * k is irrelevant here.
 */
final class EdgeDB(location: String, buckets: Int, unpacker: DistinctByteBucket.Unpacker)
  extends ByteBucketDB[DistinctByteBucket](location, EdgeDB.dbOptions(buckets), unpacker, 0) {

  def newBucket(values: Iterable[Rec]) = {
    val seed = values.head
    val b = new DistinctByteBucket(seed, List(seed), unpacker.itemSize)
    b.insertBulk(values.tail).getOrElse(b)
  }

  def visitEdges(f: List[(Rec, Rec)] => Unit) {
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

  //256 byte alignment
  //40G mmap
  def options(buckets: Int) = s"#bnum=$buckets#apow=8"
  def mmapOptions(buckets: Int) = s"${options(buckets)}#msiz=$c40g"

}

/**
 * BucketDB that merges k-mers into contiguous paths.
 * Buckets will contain lists of paths, not KMers.
 * The abundance of each k-mer is also tracked in an auxiliary database (AbundanceDB).
 *
 * The abundance filter, if present, affects some of the accessor methods.
 */
final class SeqBucketDB(location: String, options: String, buckets: Int, val k: Int, minAbundance: Option[Abundance])
extends StringBucketDB[PackedSeqBucket](location, options,
    new PackedSeqBucket.Unpacker(location, minAbundance, buckets), k) {

  def abundDB = unpacker.asInstanceOf[PackedSeqBucket.Unpacker].abundDB

  //Traversing the abundances should be cheaper than traversing the full buckets
  //for counting the number of sequences
  override def bucketSizeHistogram(limitMax: Option[Long] = None) = {
    var sizes = Vector[Int]()
    abundDB.visitBucketsReadonly((key, bucket) => {
      sizes :+= bucket.abundances.size
    })
    new Histogram(sizes, 10, limitMax)
  }

  def newBucket(values: Iterable[String]) =
    new PackedSeqBucket(Array(), Array(), k).insertBulk(values).get

  /*
   * Going purely through the abundance DB is cheaper than unpacking all sequences
   */
  def visitKeysReadonly(f: (String) => Unit) {
    minAbundance match {
      case Some(m) =>
        abundDB.visitBucketsReadonly((key, b) => {
          if (b.hasMinAbundance(m)) {
            f(key)
          }
        })
      case None =>
        abundDB.visitBucketsReadonly((key, b) => f(key))
    }
  }

  /**
   * Only write back sequences if they did actually change
   */
  override protected def shouldWriteBack(key: String, bucket: PackedSeqBucket): Boolean =
    bucket.sequencesUpdated

  override protected def beforeBulkLoad(keys: Iterable[String]) {
    abundDB.bulkLoad(keys)
  }

  /**
   * Always write back abundance when a bucket changes
   */
  override protected def afterBulkWrite(merged: Iterable[(String, PackedSeqBucket)]) {
    abundDB.setBulk(merged.map(x => (x._1 -> StringAbundanceBucket.fromAbundances(x._2.abundances).pack)))
  }

  def kmerAbundanceStats = {
    val d = new Distribution
    abundDB.visitBucketsReadonly((key, b) => {
      d.observe(b.kmerAbundances.map(_.toInt))
    })
    d
  }

  def kmerStats = {
    val r = new Distribution
    abundDB.visitBucketsReadonly((key, bucket) => {
      r.observe(bucket.abundances.map(_.length).sum)
    })
    r
  }
}
