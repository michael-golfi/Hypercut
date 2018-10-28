package dbpart.ubucket
import scala.collection.JavaConverters._
import kyotocabinet._

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
  def setInsertSingle(oldSet: String, value: String): String = 
    s"$oldSet$separator$value"  
  
  def setInsertBulk(oldSet: String, values: Iterable[String]): String = {
    val newVals = values.mkString(separator)
    setInsertSingle(oldSet, newVals)
  }
  
  def newSet(values: Iterable[String]) =
    values.mkString(separator)
  
  
  def addSingle(key: String, record: String) {    
    val v = Option(db.get(key))
    v match {
      case Some(value) =>
        if (value.size > 2000) {
          println(s"Key $key size ${value.size}")
        }
        db.set(key, setInsertSingle(value, record))
      case None => 
        db.set(key, record)
    }
    
    db.cursor()
  }
  
  import scala.collection.{Map => CMap}
  import scala.collection.mutable.{Map => MMap}
  
  def merge(into: MMap[String, String], from: CMap[String, Iterable[String]]) = {
    for ((k, vs) <- from) {
      into.get(k) match {
        case Some(existingSet) =>
          into += k -> setInsertBulk(existingSet, vs)
        case None =>
          into += k -> newSet(vs)
      }
    }
    into
  }
  
  def addBulk(data: Iterable[(String, String)]) {
    val insert = data.groupBy(_._1).mapValues(vs => vs.map(_._2))
    
    val keys = insert.keys
    val existing = db.get_bulk(keys.toList.asJava, false)
    val merged = merge(existing.asScala, insert)
    db.set_bulk(merged.asJava, false)
  }
  
}

/**
 * BucketDB implementation that deduplicates entries.
 */
class UBucketDB(location: String, options: String, separator: String = "\n") extends BucketDB(location, options, separator) {

  override def setInsertSingle(oldSet: String, value: String): String = 
    setInsertBulk(oldSet, Seq(value))  
  
  override def setInsertBulk(oldSet: String, values: Iterable[String]): String = {
    val old = Set() ++ oldSet.split(separator, -1)
    val newSet = old ++ values
    newSet.mkString(separator)    
  }
  
  override def newSet(values: Iterable[String]) =    
    values.toSeq.distinct.mkString(separator)
  
}

object UBucketDB {
  
  def main(args: Array[String]) {

    val c1g = 1l * 1204 * 1204 * 1024
    val c2g = 2 * c1g
    val c20g = 20l * c1g

    //500k buckets (approx 10% of size, assuming 5 million entries)
    //default alignment (8 bytes)
    val options = s"#msiz=$c2g#bnum=500000"
  
    
    args(0) match {
      case "add" =>
        val file = args(1)
        val db = new UBucketDB(file, options)
        addFromStream(db, flatqStream(Console.in.lines().iterator().asScala))
      case _ => throw new Exception("Unxpected command")
    }    
  }
  
  def flatqStream(from: Iterator[String]) =    
    from.flatMap(s => {      
      val spl = s.split("\t", 3)
      Some((spl(1), s))      
    })    
  
  def addFromStream(db: UBucketDB, flatq: Iterator[(String, String)]) {
    for (group <- flatq.grouped(10000)) {
      db.addBulk(group)      
    }
  }
}