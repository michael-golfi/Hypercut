package dbpart
import dbpart.ubucket._
import scala.collection.JavaConversions._
import friedrich.util.formats.GraphViz
import friedrich.util.Distribution
import friedrich.util.Histogram
import dbpart.graph.CollapsedGraph
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import dbpart.graph.PathGraphBuilder
import dbpart.graph.PathPrinter


class SeqPrintBuckets(val space: MarkerSpace, val k: Int, val numMarkers: Int, dbfile: String) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new PathBucketDB(dbfile, UBucketDB.options, k)

  import scala.concurrent.ExecutionContext.Implicits.global

  def precompIterator[A](it: Iterator[A]) = new Iterator[A] {
    def nextBuffer = if (it.hasNext) Some(Future(it.next)) else None
    var buffer = nextBuffer

    def hasNext = it.hasNext || buffer != None

    def next = {
      buffer match {
        case Some(f) =>
          val rs = Await.result(f, Duration.Inf)
          buffer = nextBuffer
          rs
        case None => ???
      }
    }
  }

  def handle(reads: Iterator[(String, String)]) {
    val handledReads =
      reads.grouped(100000).map(group =>
      { group.par.flatMap(r => handle(r._1))
      })

    for (rs <- precompIterator(handledReads)) {
      db.addBulk(rs.seq)
    }
  }

  //TODO: also handle the reverse complement of each read
  //TODO: mate pairs

  def handle(read: String) = {
    val kmers = read.sliding(k)
    val mss = extractor.markerSetsInRead(read)
    if (extractor.readCount % 10000 == 0) {
      extractor.synchronized {
        print(".")
      }
    }

    mss.map(_.packedString).iterator zip kmers
  }
}

object SeqPrintBuckets {
  val space = MarkerSpace.default

  def makeGraph(kms: KmerSpace, sbs: SeqPrintBuckets) = {
    val graph = new FastAdjListGraph[MarkerSet]
    for (key <- sbs.db.bucketKeys) {
      try {
        val n = asMarkerSet(key)
        graph.addNode(n)
        kms.add(n)
      } catch {
        case e: Exception =>
          Console.err.println(s"Warning: error while handling key '$key'")
          e.printStackTrace()
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

        Stats.begin()
        val spb = new SeqPrintBuckets(space, k, numMarkers, dbfile)
        spb.handle(
          FlatQ.stream(Console.in.lines().iterator))
        Stats.end("Build buckets")

        println("")
        var hist = spb.db.bucketSizeHistogram()
        hist.print("Bucket size (sequences)")
        hist = spb.db.kmerHistogram
        hist.print("Bucket size (kmers)")
      case "graph" =>
        Stats.begin()
        var kms = new KmerSpace()
        val k = args(1).toInt //e.g. 31
        val numMarkers = args(2).toInt //e.g. 4
        val dbfile = args(3)
        val buckets = new SeqPrintBuckets(space, k, numMarkers, dbfile)
        val graph = makeGraph(kms, buckets)
        Stats.end("Construct graph")
        kms = null //Recover memory

        Stats.begin()
        val mg = new MacroGraph(graph)
        var parts = mg.partition(10000)
        Stats.end("Partition graph")

        val hist = new Histogram(parts.map(_.size), 20)
        hist.print("Macro partition # marker sets")
//        val numSeqs = parts.map(p => buckets.db.getBulk(p.map(_.packedString)).map(_._2.size).sum)
//        new Histogram(numSeqs, 20).print("Macro partition # sequences")
//        val seqLength = parts.map(p => buckets.db.getBulk(p.map(_.packedString)).map(_._2.map(_.length).sum).sum)
//        new Histogram(seqLength, 20).print("Macro partition total sequence length")

//        Stats.begin()
//        val colGraph = CollapsedGraph.construct(parts, graph)
//        Stats.end("Collapse graph")

//        GraphViz.writeUndirected[CollapsedGraph.G[MarkerSet]](colGraph, "out.dot",
//            ms => ms.nodes.size + ":" + ms.nodes.head.packedString)

        val pp = new PathPrinter("hypercut.fasta", k)
        var pcount = 0
        var lengths = List[Int]()
        val minLength = 50
        val minPrintLength = 65

        parts = mg.collapse(1000, parts)
        Stats.begin()
        //Collapse small partitions and then iterate over the result
        for (p <- parts.par) {
          pcount += 1

          val pathGraph = new PathGraphBuilder(buckets.db, List(p), graph).result
          println(s"Path graph ${pathGraph.numNodes} nodes ${pathGraph.numEdges} edges")

          val ss = pp.findSequences(pathGraph)


          for (s <- ss) {
            if (s.length >= minLength) {
              lengths ::= s.length
            }
            lengths ::= s.length
            if (s.length >= minPrintLength) {
              pp.printSequence(s"hypercut-part$pcount", s)
            }
          }
        }
        pp.close()
        Stats.end("Find and print sequences")
        new Histogram(lengths, 20).print("Contig length")
    }

  }

}