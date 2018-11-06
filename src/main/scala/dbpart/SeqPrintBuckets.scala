package dbpart
import dbpart.ubucket._
import scala.collection.JavaConversions._
import friedrich.util.formats.GraphViz

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
  
    def asMarkerSet(key: String) = MarkerSet.unpack(space, key).fixMarkers
    
    args(0) match {
      case "build" =>
        val k = args(1).toInt //e.g. 31
        val numMarkers = args(2).toInt //e.g. 4
        val dbfile = args(3)

        new SeqPrintBuckets(space, k, numMarkers, dbfile).handle(
          FlatQ.stream(Console.in.lines().iterator))
      case "graph" =>
        val graph = new FastAdjListGraph[MarkerSet]
        val kms = new KmerSpace()
        val k = args(1).toInt //e.g. 31
        val numMarkers = args(2).toInt //e.g. 4
        val dbfile = args(3)
        val buckets = new SeqPrintBuckets(space, k, numMarkers, dbfile)
        for ((key, vs) <- buckets.db.buckets) {
          try {
            val n = asMarkerSet(key)
            graph.addNode(n)
            kms.add(n.fixMarkers)
          } catch {
            case e: Exception =>
              Console.err.println(s"Warning: error while handling key '$key'")
          }
        }
        
        println(graph.numNodes + " nodes")
        
        for ((from, to) <- kms.completeEdges(space, numMarkers)) {
          graph.uncheckedAddEdge(from, to)
        }
        println(graph.numEdges + " edges")
        
        GraphViz.write[MarkerSet](graph, "out.dot", ms => ms.packedString)
    }

  }
 
}