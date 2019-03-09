package dbpart
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.blocking

import dbpart.ubucket._
import scala.collection.JavaConversions._
import friedrich.util.formats.GraphViz
import friedrich.util.Distribution
import friedrich.util.Histogram
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
  dbOptions: String = SeqBucketDB.options, minCov: Option[Int]) {
  val extractor = new MarkerSetExtractor(space, numMarkers, k)
  val db = new SeqBucketDB(dbfile, dbOptions, k, minCov)
  lazy val edgeDb = new EdgeDB(dbfile.replace(".kch", "_edge.kch"))

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

  def packEdge(e: MacroEdge) = (e._1.packedString, e._2.packedString)

  def addEdges(edgeSet: EdgeSet, edges: Iterable[Iterator[(String, String)]]) {
    blocking {
      for (es <- edges) {
        edgeSet.add(es)
      }
    }
  }

  def handle(reads: Iterator[String], index: Boolean) {
    val edgeSet = new EdgeSet
    var edgesFuture: Future[Unit] = Future.successful(())

    /**
     * A larger buffer size costs memory and GC activity,
     * but helps optimise disk access
     */
    val bufferSize = if (index) 50000 else 5000
    val handledReads =
      reads.grouped(bufferSize).map(group =>
      { group.par.map(r => {
        val forward = extractor.handle(r)
        val rev = extractor.handle(DNAHelpers.reverseComplement(r))
        (forward._1 ++ rev._1,
            forward._2.iterator.map(packEdge) ++ rev._2.iterator.map(packEdge))
      })
      })

    if (index) {
      for {
        segment <- handledReads
        data = segment.flatMap(_._1)
        edges = segment.map(_._2).seq
      } {
        val st = Stats.beginNew
        db.addBulk(data.seq)
        st.end("Write data chunk")
        edgesFuture = edgesFuture.andThen { case u => addEdges(edgeSet, edges) }
      }
    } else {
      for {
        segment <- handledReads
        edges = segment.map(_._2).seq
      } {
        edgesFuture = edgesFuture.andThen { case u => addEdges(edgeSet, edges) }
      }
    }

    Await.result(edgesFuture, Duration.Inf)
    val st = Stats.beginNew
    edgeSet.writeTo(edgeDb)
    st.end("Write edges")
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

  def build(inputFile: String, matesFile: Option[String], index: Boolean) {
    Stats.begin()
    handle(FastQ.iterator(inputFile), index)
    Stats.end("Build buckets")
    println("")
  }

  def stats() {
    var hist = db.bucketSizeHistogram(Some(200L))
    hist.print("Bucket size (sequences)")
    //hist = spb.db.kmerHistogram
    //hist.print("Bucket size (kmers)")
    var dist = db.kmerCoverageStats
    dist.print("Kmer coverage")
    hist = edgeDb.bucketSizeHistogram()
    hist.print("Macro graph node degree (no coverage threshold)")
  }

  def asMarkerSet(key: String) = MarkerSet.unpack(space, key).fixMarkers.canonical

  def makeGraph() = {
    val graph = new DoublyLinkedGraph[MarkerSet]
    var nodeLookup = new scala.collection.mutable.HashMap[String, MarkerSet]

    //This will produce the coverage filtered set of nodes
    for (key <- db.bucketKeys) {
      try {
        val n = asMarkerSet(key)
        graph.addNode(n)
        nodeLookup += (n.packedString -> n)
      } catch {
        case e: Exception =>
          Console.err.println(s"Warning: error while handling key '$key'")
          e.printStackTrace()
      }
    }
    println(graph.numNodes + " nodes")

//    val allPossibleEdges = (for (a <- graph.nodes; b <- graph.nodes; if a != b) yield (a,b))
//    val edges = validateEdges(allPossibleEdges)
//    val edges = validateEdges(kmerSpace.completeEdges(space, numMarkers))

    /*
     * Note: marker sets currently use reference equality only (no deep structural equality)
     * so it is necessary to reuse the same objects.
     */
    var count = 0
    val edges = edgeDb.allEdges(nodeLookup.get)
    for {
      (from, to) <- edges
      filtFrom <- from
      filtTo <- to
    } {
      graph.uncheckedAddEdge(filtFrom, filtTo)
      count += 1
      if (count % 100000 == 0) {
        println(s"${graph.numNodes} nodes ${graph.numEdges} edges")
      }
    }
    nodeLookup = null

    println(s"${graph.numEdges} edges (filtered out $filteredOutEdges)")

    Distribution.printStats("Node degree", graph.nodes.map(graph.degree))

    val hist = new Histogram(Seq() ++ graph.nodes.map(graph.degree))
    hist.print("Node degree")
    graph
  }

  var filteredOutEdges = 0

  /**
   * Testing/validation operation for edges. Verifies that edges in the macro graph
   * correspond to overlapping k-mers in the actual de Bruijn graph.
   * Slow and memory intensive.
   */
  def validateEdges(edges: Iterator[(MarkerSet, MarkerSet)]) = {
    val allHeads = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(0, k - 1))))
    val allTails = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(1)).toSet))
    edges.filter(e => {
      val ts = allTails(e._1.packedString)
      val hs = allHeads(e._2.packedString)
      val pass = (e._1 != e._2) && hs.exists(ts.contains)
      if (!pass) {
        filteredOutEdges += 1
      }
      pass
    })
  }

  def list() {
    for (b <- db.bucketKeys) println(b)
  }

  def show(buckets: Iterable[String]) {
    for { b <- buckets } show(b)
  }

  def show(bucket: String) {
    println(s"Bucket $bucket")
    val seqs = db.getBulk(List(bucket))
    val edges = edgeDb.getBulk(List(bucket))

    val ind = "  "
    //Note: could also print sequence average coverage, integer coverages etc.
    for {
      b <- seqs.headOption
      sc <- (b._2.sequences zip b._2.coverage.coverages)
    } {
      println(ind + sc._1)
      println(ind + sc._2)
    }
    for {
      e <- edges.headOption
    } {
      println(ind + "Edges forward: " + e._2.items.mkString(" "))
    }
  }
}

object SeqPrintBuckets {
  val space = MarkerSpace.default
}