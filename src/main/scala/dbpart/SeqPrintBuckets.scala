package dbpart
import dbpart.ubucket._
import scala.collection.JavaConversions._

class SeqPrintBuckets(space: MarkerSpace, k: Int, numMarkers: Int, dbfile: String) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new UBucketDB(dbfile, UBucketDB.options)
  
  def handle(reads: Iterator[(String, String)]) {
    for (rs <- reads.grouped(100000);
        chunk = rs.toSeq.par.flatMap(r => handle(r._1))) {
      db.addBulk(chunk.seq)
    }
  }
  
  def handle(read: String) = {
    val kmers = read.sliding(k)
    val mss = extractor.markerSetsInRead(read)
    if (extractor.readCount % 10000 == 0) {
      print(".")
    }
    
    mss.map(_.packedString).iterator zip kmers
  }
}

object SeqPrintBuckets {
  val space = MarkerSpace.default
  
  def main(args: Array[String]) {
    val k = args(0).toInt //e.g. 31
    val numMarkers = args(1).toInt //e.g. 4
    val dbfile = args(2)
    
    new SeqPrintBuckets(space, k, numMarkers, dbfile).handle(
      FlatQ.stream(Console.in.lines().iterator)
      )
  }
 
}