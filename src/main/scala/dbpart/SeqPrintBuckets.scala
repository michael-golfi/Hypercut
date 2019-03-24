package dbpart
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration

import dbpart.ubucket._
import friedrich.util.Distribution
import friedrich.util.Histogram
import miniasm.genome.util.DNAHelpers
import dbpart.graph.MacroNode
import dbpart.graph.DoubleArrayListGraph

final class SeqPrintBuckets(val space: MarkerSpace, val k: Int, numMarkers: Int,
  dbfile: String, dbOptions: String = SeqBucketDB.options, minCov: Option[Int]) {

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

  def packEdge(e: ExpandedEdge) = {
    (e._1.compact, e._2.compact)
  }

  def addEdges(edgeSet: EdgeSet, edges: Iterable[Iterator[CompactEdge]]) {
    blocking {
      for (es <- edges) {
        edgeSet.add(es)
      }
    }
    println("Up to " + edgeSet.seenNodes + " nodes found")
  }

  def handle(reads: Iterator[String], index: Boolean) {
    val edgeFlushInt = Some(5000000)
    val edgeSet = new EdgeSet(edgeDb, edgeFlushInt, space)
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

    for {
      segment <- handledReads
      edges = segment.map(_._2).seq
    } {
      if (index) {
        val st = Stats.beginNew
        blocking {
          db.addBulk(segment.flatMap(_._1).seq)
        }
        st.end("Write data chunk")
      }
      //Synchronize here to ensure we don't proceed if the edge set is currently writing
      edgeSet.writeLock.synchronized {
        edgesFuture = edgesFuture.andThen { case u => addEdges(edgeSet, edges) }
      }
    }

    Await.result(edgesFuture, Duration.Inf)
    val st = Stats.beginNew
    edgeSet.writeTo(edgeDb, space)
    st.end("Write edges")
  }

  def checkConsistency(kmerCheck: Boolean) {
    import scala.collection.mutable.HashMap

    var errors = 0
    var count = 0
    /*
     * Check that each k-mer appears in only one bucket.
     * Expensive, memory intensive operation. Intended for debug purposes.
     */
    var map = new HashMap[String, String]()
    for ((key, bucket) <- db.buckets) {

      if (bucket.sequences.size > 100) {
        println(s"Warning: bucket $key contains ${bucket.sequences.size} sequences")
      }

      val numCoverages = bucket.coverage.coverages.size
      val numSeqs = bucket.sequences.size

      if (numCoverages != numSeqs) {
        Console.err.println(s"Error: bucket $key has $numSeqs sequences but $numCoverages coverages")
        errors += 1
      }

      /*
       * Also check the validity of each key
       */
      if (!checkKey(key)) {
        Console.err.println(s"Error: key $key is incorrect")
        errors += 1
      }

      if (kmerCheck) {
        for (kmer <- bucket.kmers) {
          count += 1
          if (map.contains(kmer)) {
            Console.err.println(s"Error: $kmer is contained in two buckets: $key, ${map(kmer)}")
            errors += 1
          }

          map += (kmer -> key)
          if (count % 10000 == 0) {
            print(".")
          }
        }
        val numKmers = bucket.kmers.size
        val numCoverages = bucket.coverage.coverages.map(_.length).sum
        if (numKmers != numCoverages) {
          Console.err.println(s"Error: bucket $key has $numKmers kmers but $numCoverages coverage positions")
        }
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
    handle(ReadFiles.iterator(inputFile), index)
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
  def asMacroNode(key: String) = new MacroNode(MarkerSet.unpack(space, key).compact)

  def makeGraph() = {
    var nodeLookup = new scala.collection.mutable.HashMap[MacroNode, MacroNode]

    db.visitKeysReadonly(key => {
      try {
        val ms = asMarkerSet(key)
        val n = new dbpart.graph.MacroNode(ms.compact)
        nodeLookup += (n -> n)
      } catch {
        case e: Exception =>
          Console.err.println(s"Warning: error while handling key '$key'")
          e.printStackTrace()
      }
    })

    val graph = new DoubleArrayListGraph[MacroNode](nodeLookup.keySet.toArray)
    println(graph.numNodes + " nodes")

//    val allPossibleEdges = (for (a <- graph.nodes; b <- graph.nodes; if a != b) yield (a,b))
//    val edges = validateEdges(allPossibleEdges)
//    val edges = validateEdges(kmerSpace.completeEdges(space, numMarkers))

    /*
     * Reusing the same node objects for efficiency
     */
    var count = 0
    edgeDb.visitEdges(edges => {
      val es = edges.map(x => (nodeLookup.get(asMacroNode(x._1)), nodeLookup.get(asMacroNode(x._2))))
      for {
        (from, to) <- es
        filtFrom <- from
        filtTo <- to
      } {
        graph.addEdge(filtFrom, filtTo)
        count += 1
        if (count % 1000000 == 0) {
          println(s"${graph.numNodes} nodes ${graph.numEdges} edges")
        }
      }
    })
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
  def validateEdges(edges: Iterator[MacroEdge]) = {
    val allHeads = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(0, k - 1))))
    val allTails = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(1)).toSet))
    edges.filter(e => {
      val ts = allTails(e._1.uncompact(space))
      val hs = allHeads(e._2.uncompact(space))
      val pass = (e._1 != e._2) && hs.exists(ts.contains)
      if (!pass) {
        filteredOutEdges += 1
      }
      pass
    })
  }

  def list() {
    db.visitKeysReadonly(key => println(key))
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