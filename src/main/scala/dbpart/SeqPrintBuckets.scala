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
import dbpart.graph.PathNode

final class SeqPrintBuckets(val space: MarkerSpace, val k: Int, val numMarkers: Int, dbfile: String,
  dbOptions: String = BucketDB.options, minCov: Option[Int]) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new SeqBucketDB(dbfile, dbOptions, k, minCov)

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
    val kmers = Read.kmers(read, k)
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

  def asMarkerSet(key: String) = MarkerSet.unpack(space, key).fixMarkers.canonical

  def makeGraph(kms: KmerSpace) = {
    val graph = new DoublyLinkedGraph[MarkerSet]
    for (key <- db.bucketKeys) {
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
    for ((from, to) <- validateEdges(kms.completeEdges(space, numMarkers))) {
      graph.uncheckedAddEdge(from, to)
    }
    println(s"${graph.numEdges} edges (filtered out $filteredOutEdges)")

    Distribution.printStats("Node degree", graph.nodes.map(graph.degree))

    val hist = new Histogram(Seq() ++ graph.nodes.map(graph.degree))
    hist.print("Node degree")
    graph
  }

  var filteredOutEdges = 0

  //Testing operation that is probably slow and memory intensive.
  //Should eventually be replaced by a dedicated edges database built at
  //insertion time.
  def validateEdges(edges: Iterator[(MarkerSet, MarkerSet)]) = {
    val allHeads = Map() ++ db.buckets.map(x => (x._1, x._2.heads))
    val allTails = Map() ++ db.buckets.map(x => (x._1, x._2.tails))
    edges.filter(e => {
      val ts = allTails(e._1.packedString).toSet
      val hs = allHeads(e._2.packedString)
      val pass = hs.exists(ts.contains)
      if (!pass) {
        filteredOutEdges += 1
      }
      pass
    })
  }

  def makeGraphFindPaths(partitionGraphs: Boolean) {
    var kms = new KmerSpace()
    Stats.begin()
    val graph = makeGraph(kms)
    Stats.end("Construct graph")
    kms = null //Recover memory

    Stats.begin()
    val partBuild = new PartitionBuilder(graph)
    var parts = partBuild.partition(25000)
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

        parts = partBuild.collapse(25000, parts)
        findPaths(graph, parts, partitionGraphs)
  }

  def findPaths(
    graph: Graph[MarkerSet],
    parts: List[List[MarkerSet]],
    debugGraphs: Boolean) {
    val pp = new PathPrinter("hypercut.fasta", k)

    var pcount = 0

    @volatile
    var lengths = List[Int]()
    val minLength = 50
    val minPrintLength = 65
    Stats.begin()
    for (p <- parts.par) {
      pcount += 1

      val pathGraph = new PathGraphBuilder(db, List(p), graph).result
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

      if (debugGraphs) {
        GraphViz.writeUndirected(pathGraph, s"part$pcount.dot", (n: PathNode) => n.seq)
      }
    }
    pp.close()
    Stats.end("Find and print sequences")
    new Histogram(lengths, 20).print("Contig length")
  }

}

object SeqPrintBuckets {
  val space = MarkerSpace.default
}