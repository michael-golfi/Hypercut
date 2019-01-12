package dbpart.ubucket
import scala.collection.JavaConverters._
import kyotocabinet._
import dbpart.FlatQ
import friedrich.util.Distribution
import friedrich.util.Histogram
import scala.collection.mutable.ArrayBuffer

/**
 * Database of unique buckets for k-mers.
 * k-mers with identical sequence data are stored together.
 */
class BucketDB(location: String, options: String, separator: String = "\n") {
  
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
  
  
  /**
   * The default bucket implementation simply adds to a list.
   * Overriding methods can be more sophisticated, e.g. perform sorting or deduplication
   */
  def setInsertSingle(oldSet: String, value: String): Option[String] = 
    Some(s"$oldSet$separator$value")  
  
  /**
   * Insert a number of values into the set, returning the updated set if an update is necessary.
   * Returns None if no update is needed.
   */
  def setInsertBulk(oldSet: String, values: Iterable[String]): Option[String] = {
    val newVals = values.mkString(separator)
    setInsertSingle(oldSet, newVals)
  }
  
  def newSet(values: Iterable[String]) =
    values.mkString(separator)
  
  def unpackSet(set: String): Iterable[String] = set.split(separator, -1)    
    
  def addSingle(key: String, record: String) {    
    val v = Option(db.get(key))
    v match {
      case Some(value) =>
        if (value.size > 2000) {
          println(s"Key $key size ${value.size}")
        }
        setInsertSingle(value, record) match {
          case Some(nv) => db.set(key, nv)
          case None =>
        }
      case None => 
        db.set(key, record)
    }
    
    db.cursor()
  }
  
  import scala.collection.{Map => CMap}
  import scala.collection.mutable.{Map => MMap}
  
  def merge(oldVals: MMap[String, String], from: CMap[String, Iterable[String]]) = {
    var r = MMap[String, String]()    
    for ((k, vs) <- from) {
      oldVals.get(k) match {
        case Some(existingSet) =>
          setInsertBulk(existingSet, vs) match {
            case Some(ins) => 
              r += k -> ins              
            case None =>
          }          
        case None =>
          r += k -> newSet(vs)          
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
  
  def getBulk(keys: Iterable[String]): CMap[String, Iterable[String]] = {
    db.get_bulk(seqAsJavaList(keys.toSeq), false).asScala.mapValues(unpackSet)    
  }
  
  def buckets: Iterator[(String, Iterable[String])] = {

    new Iterator[(String, Iterable[String])] {
      val cur = db.cursor()
      cur.jump()      
      var continue = cur.step()
      var nextVal: Array[String] = cur.get_str(true)

      override def next() = {
        val r = (nextVal(0), unpackSet(nextVal(1)))
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

  def bucketSizeStats() = {
    val r = new Distribution    
    for ((b, vs) <- buckets) {
      r.observe(vs.size)
    }
    r
  }
  
  def bucketSizeHistogram() = {
    val ss = buckets.map(_._2.size)
    val h = new Histogram(ss.toSeq)
    h
  }

}

/**
 * BucketDB implementation that deduplicates entries.
 */
class UBucketDB(location: String, options: String, separator: String = "\n") 
extends BucketDB(location, options, separator) {

  override def setInsertSingle(oldSet: String, value: String): Option[String] = 
    setInsertBulk(oldSet, Seq(value))  
  
  import scala.collection.mutable.{Set => MSet}
  override def setInsertBulk(oldSet: String, values: Iterable[String]): Option[String] = {
    val old = MSet() ++ unpackSet(oldSet)
    val oldSize = old.size
    val newSet = old ++ values
    if (newSet.size == oldSize) {
      None
    } else {
      Some(newSet.mkString(separator))
    }
  }
  
  override def newSet(values: Iterable[String]) =    
    values.toSeq.distinct.mkString(separator)
  
}

/**
 * BucketDB that merges k-mers into contiguous paths.
 */
class PathBucketDB(location: String, options: String, k: Int, separator: String = "\n") 
extends BucketDB(location, options, separator) {
  
  override def setInsertSingle(oldSet: String, value: String): Option[String] = 
    setInsertBulk(oldSet, Seq(value))
 
  override def setInsertBulk(oldSet: String, values: Iterable[String]): Option[String] = {
    val old = unpackSet(oldSet)
    var r: ArrayBuffer[String] = new ArrayBuffer(values.size + old.size)
    r ++= old
    var updated = false    
    for (v <- values; if !seqExists(v, r)) {      
      insertSequence(v, r)
      updated = true
    }
    if (updated) {
      Some(r.mkString(separator))
    } else {
      None
    }
  }
  
  override def newSet(values: Iterable[String]) = {
    setInsertBulk(values.head, values.tail).getOrElse(values.head)
  }

  final def seqExists(data: String, in: Iterable[String]): Boolean = {
    val it = in.iterator
    while (it.hasNext) {
      val s = it.next
      if (s.indexOf(data) != -1) {
        return true
      }
    }
    false
  }
   
  /**
   * Insert a new sequence into a set of pre-existing sequences, by merging if possible.
   */
  final def insertSequence(data: String, into: ArrayBuffer[String]) {    
    
    val suffix = data.substring(1)
    val prefix = data.substring(0, k - 1)
    var i = 0
    while (i < into.size) {
      val existingSeq = into(i)
      if (existingSeq.startsWith(suffix)) {
        into(i) = (data.charAt(0) + existingSeq)
        return
      }
      if (existingSeq.endsWith(prefix)) {
        into(i) = (existingSeq + data.charAt(data.length() - 1))
        return
      } 
      i += 1
    }        
    into += data    
  }
}

object UBucketDB {

  val c1g = 1L * 1204 * 1204 * 1024
  val c4g = 4 * c1g
  val c8g = 8 * c1g
  val c20g = 20L * c1g

  //40M buckets 
  //256b byte alignment
  //8G mmap
  //#comp=zlib
  val options = s"#msiz=$c8g#bnum=40000000#apow=8"
  
  def main(args: Array[String]) {
   
    args(0) match {
      case "add" =>
        val file = args(1)
        val db = new UBucketDB(file, options)
        addFromStream(db, FlatQ.stream(Console.in.lines().iterator().asScala))
      case "stats" =>
        val file = args(1)
        val db = new UBucketDB(file, options)
        db.bucketSizeStats().print()
        db.bucketSizeHistogram().print("Bucket sizes")
      case "graph" =>
        val file = args(1)
        val db = new UBucketDB(file, options)
        
      case _ => throw new Exception("Unxpected command")
    }    
  }
  
  
  val k = 31
  def kmers(read: String) = read.sliding(k)
  //2^20 = 1M possible keys
  def key(kmer: String) = kmer.substring(0, 10)
    
  def addFromStream(db: UBucketDB, flatq: Iterator[(String, String)]) {
    for (group <- flatq.grouped(10000);
        kvs = group.map(x => (key(x._1), x._1))) {
      db.addBulk(kvs)      
    }
  }
}