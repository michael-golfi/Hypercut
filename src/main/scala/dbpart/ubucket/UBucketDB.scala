package dbpart.ubucket
import scala.collection.JavaConverters._
import kyotocabinet._

/**
 * Database of unique buckets for k-mers.
 * k-mers with identical sequence data are stored together.
 */
class UBucketDB(location: String, options: String, separator: String = "\n") {
  
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
  
  
  def addSingle(key: String, record: String) {    
    val v = Option(db.get(key))
    v match {
      case Some(value) =>
        if (value.size > 2000) {
          println(s"Key $key size ${value.size}")
        }
        db.set(key, s"$value$separator$record")
      case None => 
        db.set(key, record)
    }
    
    db.cursor()
  }
  
  import scala.collection.{Map => CMap}
  import scala.collection.mutable.{Map => MMap}
  
  def merge(into: MMap[String, String], d2: CMap[String, String]) = {
    for ((k, v) <- d2) {
      into.get(k) match {
        case Some(v1) =>
          into += k -> s"$v1$separator$v"
        case None =>
          into += k -> v
      }
    }
    into
  }
  
  def addBulk(data: Iterable[(String, String)]) {
    val insert = data.groupBy(_._1).
      mapValues(vs => vs.map(_._2).mkString(separator))
    
    val keys = insert.keys
    val existing = db.get_bulk(keys.toList.asJava, false)
    val merged = merge(existing.asScala, insert)
    db.set_bulk(merged.asJava, false)
  }
  
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