package hypercut.bucketdb
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import hypercut.bucket._
import hypercut.graph.DoubleArrayListGraph
import hypercut.hash._
import hypercut.shortread.ReadFiles
import friedrich.util.Distribution
import friedrich.util.Histogram
import miniasm.genome.util.DNAHelpers
import hypercut._
import scala.collection.Seq

final class BucketDBTool(val space: MotifSpace, val k: Int,
                         settings: Settings, dbOptions: String, minAbund: Option[Abundance]) {

  val extractor = new MotifSetExtractor(space, k)
  lazy val db = new SeqBucketDB(settings.dbfile, dbOptions, settings.buckets, k, minAbund)
  lazy val edgeDb = settings.edgeDb(space)

  import scala.concurrent.ExecutionContext.Implicits.global

  def packEdge(e: ExpandedEdge) = {
    (e._1.compact, e._2.compact)
  }

  def addEdges(edgeSet: EdgeSet, edges: TraversableOnce[List[MotifSet]]) {
    edgeSet.add(edges)
    println("Up to " + edgeSet.seenNodes + " nodes found")
  }

  def handle(reads: Iterator[String], doIndex: Boolean, doEdges: Boolean) {
    val edgeSet = new EdgeSet(edgeDb, settings.edgeWriteInterval)
    var edgesFuture: Future[Unit] = Future.successful(())

    /**
     * A larger buffer size costs memory and GC activity,
     * but helps optimise disk access
     */
    val bufferSize = settings.readBufferSize
    val handledReads =
      reads.grouped(bufferSize).map(group =>
      { group.par.flatMap(r => {
        val forward = extractor.motifs(r)
        val rev = extractor.motifs(DNAHelpers.reverseComplement(r))
        Seq(forward, rev)
      })
      })

    for {
      segment <- handledReads
      edges = segment.seq.iterator.map(_._1)
    } {
      if (doIndex) {
        val st = Stats.beginNew
        blocking {
          db.addBulk(segment.flatMap(x => x._1.map(_.packedString).iterator zip x._2).seq)
        }
        st.end("Write data chunk")
      }
      if (doEdges) {
        Await.result(edgesFuture, Duration.Inf)
        edgesFuture = Future { addEdges(edgeSet, edges) }
      }
    }

    Await.result(edgesFuture, Duration.Inf)
    val st = Stats.beginNew
    edgeSet.writeTo(edgeDb)
    st.end("Write edges")
  }

  def checkConsistency(kmerCheck: Boolean, seqCheck: Boolean) {
    val c = new Checker(space, k, kmerCheck, seqCheck)

    db.visitBucketsReadonly((key, bucket) => {
      try {
        c.checkBucket(key, bucket)
      } catch {
        case e: Exception =>
          Console.err.println(s"Error while checking bucket $key")
          println(s"Sequences ${bucket.sequences.toList}")
          e.printStackTrace()
      }
    })
    println(s"Check finished. ${c.errors} errors found.")
  }

  def build(inputFile: String, matesFile: Option[String], index: Boolean, edges: Boolean) {
    Stats.begin()
    handle(ReadFiles.iterator(inputFile), index, edges)
    Stats.end("Build buckets")
    println("")
  }

  def stats() {
    var hist = db.bucketSizeHistogram(Some(200L))
    hist.print("Bucket size (sequences)")
    //hist = spb.db.kmerHistogram
    //hist.print("Bucket size (kmers)")
    var dist = db.kmerAbundanceStats
    dist.print("Kmer abundance")
    hist = edgeDb.bucketSizeHistogram()
    hist.print("Macro graph node degree (no abundance threshold)")
  }

  var filteredOutEdges = 0

  /**
   * Testing/validation operation for edges. Verifies that edges in the macro graph
   * correspond to overlapping k-mers in the actual de Bruijn graph.
   * Slow and memory intensive.
   *
   * TODO: reimplement in terms of visitors
   */
//  def validateEdges(edges: Iterator[MacroEdge]) = {
//    val allHeads = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(0, k - 1))))
//    val allTails = Map() ++ db.buckets.map(x => (x._1, x._2.kmers.map(_.substring(1)).toSet))
//    edges.filter(e => {
//      val ts = allTails(e._1.uncompact(space))
//      val hs = allHeads(e._2.uncompact(space))
//      val pass = (e._1 != e._2) && hs.exists(ts.contains)
//      if (!pass) {
//        filteredOutEdges += 1
//      }
//      pass
//    })
//  }

  def list() {
    db.visitKeysReadonly(key => println(key))
  }

  def show(buckets: Iterable[String]) {
    for { b <- buckets } show(b)
  }

  def show(bucket: String) {
    println(s"Bucket $bucket")
    val key = MotifSet.unpackToCompact(space, bucket)
    val seqs = db.getBulk(List(bucket))
    val edges = edgeDb.getBulk(List(key.data))

    val ind = "  "
    //Note: could also print sequence average abundance, integer abundances etc.
    for {
      b <- seqs.headOption
      sc <- (b._2.sequences zip b._2.abundances)
    } {
      println(ind + sc._1)
      println(ind + sc._2)
    }
    for {
      e <- edges.headOption
    } {
      println(ind + "Edges forward: " + e._2.items.map(MotifSet.uncompactToString(_, space)).mkString(" "))
    }
  }
}
