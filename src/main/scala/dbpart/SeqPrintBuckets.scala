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
        extractor.handle(r) ++
        extractor.handle(DNAHelpers.reverseComplement(r))
      })
      })

    for (rs <- precompIterator(handledReads)) {
      db.addBulk(rs.seq)
    }
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
    val edges = validateEdges(kms.completeEdges(space, numMarkers))
    for ((from, to) <- edges) {
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
    val allHeads = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(0, k - 1))))
    val allTails = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(1)).toSet))
    edges.filter(e => {
      val ts = allTails(e._1.packedString)
      val hs = allHeads(e._2.packedString)
      val pass = hs.exists(ts.contains)
      if (!pass) {
        filteredOutEdges += 1
      }
      pass
    })
  }
}

object SeqPrintBuckets {
  val space = MarkerSpace.default
}