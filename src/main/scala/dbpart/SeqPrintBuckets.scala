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
import friedrich.graph.Graph
import miniasm.genome.util.DNAHelpers
import friedrich.util.IO

final class SeqPrintBuckets(val space: MarkerSpace, val k: Int, val numMarkers: Int, dbfile: String,
  dbOptions: String = BucketDB.options) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new SeqBucketDB(dbfile, dbOptions, k)

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

  def handle(reads: Iterator[String]) {
    val handledReads =
      reads.grouped(50000).map(group =>
      { group.par.flatMap(r => {
        handle(r) ++
        handle(DNAHelpers.reverseComplement(r))
      })
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

  def checkConsistency() {
    import scala.collection.mutable.HashMap

    var errors = 0
    var count = 0
    /*
       * Check that each k-mer appears in only one bucket.
       * Expensive, memory intensive operation. Intended for debug purposes.
       */
    var map = new HashMap[String, String]()
    for ((key, bucket) <- db.buckets; kmer <- bucket.kmers) {
      count += 1
      if (map.contains(kmer)) {
        Console.err.println(s"Error: $kmer is contained in two buckets: $key, ${map(kmer)}")
        errors += 1
      }
      /*
         * Also check the validity of each key
         */
      if (!checkKey(key)) {
        Console.err.println(s"Error: key $key is incorrect")
        errors += 1
      }
      map += (kmer -> key)
      if (count % 10000 == 0) {
        print(".")
      }
    }

    println(s"Check finished. $errors errors found.")
  }

  def checkKey(key: String): Boolean = {
    try {
      val ms = MarkerSet.unpack(space, key)
      if (ms.relativeMarkers(0).pos != 0) {
        return false
      }
      for (
        sub <- ms.relativeMarkers.sliding(2);
        if (sub.length >= 2)
      ) {
        val pos1 = sub(1).pos
        val l1 = sub(0).tag.length()
        if (pos1 < l1) {
          //Markers cannot overlap
          return false
        }
      }
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  def build(inputFile: String, matesFile: Option[String]) {
    Stats.begin()
    handle(FastQ.iterator(inputFile))
    Stats.end("Build buckets")
    println("")
  }

  def stats() {
    val hist = db.bucketSizeHistogram()
    hist.print("Bucket size (sequences)")
    //hist = spb.db.kmerHistogram
    //hist.print("Bucket size (kmers)")
    var dist = db.kmerCoverageStats
    dist.print("Kmer coverage")
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
        val partBuild = new PartitionBuilder(graph)
        var parts = partBuild.partition(2000)
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

        parts = partBuild.collapse(1000, parts)
        findPaths(k, buckets, graph, parts)
    }

  }

  def findPaths(k: Int, buckets: SeqPrintBuckets, graph: Graph[MarkerSet],
                parts: List[List[MarkerSet]]) {
    val pp = new PathPrinter("hypercut.fasta", k)

    var pcount = 0

    @volatile
    var lengths = List[Int]()
    val minLength = 50
    val minPrintLength = 65
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