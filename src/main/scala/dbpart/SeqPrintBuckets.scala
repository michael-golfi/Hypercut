package dbpart
import dbpart.ubucket._
import scala.collection.JavaConversions._
import friedrich.util.formats.GraphViz
import friedrich.util.Distribution
import friedrich.util.Histogram
import dbpart.graph.CollapsedGraph

class SeqPrintBuckets(val space: MarkerSpace, val k: Int, val numMarkers: Int, dbfile: String) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new UBucketDB(dbfile, UBucketDB.options)
  
  def handle(reads: Iterator[(String, String)]) {
    for (rs <- reads.grouped(100000);
        chunk = rs.toSeq.par.flatMap(r => handle(r._1))) {
      db.addBulk(chunk.seq)
    }
  }
  
  //TODO: also handle the reverse complement of each read
  //TODO: mate pairs
  
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
  
  def makeGraph(kms: KmerSpace, sbs: SeqPrintBuckets) = {
    val graph = new FastAdjListGraph[MarkerSet]
    for ((key, vs) <- sbs.db.buckets) {
      try {
        val n = asMarkerSet(key)
        graph.addNode(n)
        kms.add(n)
      } catch {
        case e: Exception =>
          Console.err.println(s"Warning: error while handling key '$key'")
      }
    }

    println(graph.numNodes + " nodes")

    for ((from, to) <- kms.completeEdges(space, sbs.numMarkers)) {
      graph.uncheckedAddEdge(from, to)
    }
    println(graph.numEdges + " edges")
    
    Distribution.printStats("Node degree", graph.nodes.map(graph.degree))
    
    val hist = new Histogram(Seq() ++ graph.nodes.map(graph.degree))
    hist.print("Node degree")
    graph
  }
  
  def asMarkerSet(key: String) = MarkerSet.unpack(space, key).fixMarkers.canonical
    
  def main(args: Array[String]) {
  
    args(0) match {
      case "build" =>
        val k = args(1).toInt //e.g. 31
        val numMarkers = args(2).toInt //e.g. 4
        val dbfile = args(3)

        new SeqPrintBuckets(space, k, numMarkers, dbfile).handle(
          FlatQ.stream(Console.in.lines().iterator))
      case "graph" =>
        Stats.begin()
        val kms = new KmerSpace()
        val k = args(1).toInt //e.g. 31
        val numMarkers = args(2).toInt //e.g. 4
        val dbfile = args(3)
        val buckets = new SeqPrintBuckets(space, k, numMarkers, dbfile)
        val graph = makeGraph(kms, buckets)   
        Stats.end("Construct graph")
        
        Stats.begin()
        val mg = new MacroGraph(graph)
        val parts = mg.partition(1000)
        Stats.end("Partition graph")
        
        val hist = new Histogram(parts.map(_.size), 20)
        hist.print("Macro partition size")
        val numKmers = parts.map(p => buckets.db.getBulk(p.map(_.packedString)).map(_._2.size).sum)
        new Histogram(numKmers, 20).print("Macro partition expanded size")    
        
        Stats.begin()
        val colGraph = CollapsedGraph.construct(parts, graph)
        Stats.end("Collapse graph")
        
        GraphViz.writeUndirected[CollapsedGraph.G[MarkerSet]](colGraph, "out.dot", 
            ms => ms.nodes.size + ":" + ms.nodes.head.packedString)
    }

  }
 
}