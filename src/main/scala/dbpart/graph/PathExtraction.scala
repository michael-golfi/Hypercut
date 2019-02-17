package dbpart.graph

import dbpart.KmerSpace
import dbpart.MarkerSet
import friedrich.util.formats.GraphViz
import dbpart.PartitionBuilder
import friedrich.util.Histogram
import friedrich.graph.Graph
import dbpart.Stats
import dbpart.SeqPrintBuckets

/**
 * Encapsulates the operations of building a macro graph, partitioning it
 * into partitions of a given maximum size, and finding and printing contigs
 * from each partition.
 */
class PathExtraction(buckets: SeqPrintBuckets,
  partitionSize: Int, minPrintLength: Int, outputPartitionGraphs: Boolean,
  printReasons: Boolean) {
  val k: Int = buckets.k
  val db: dbpart.ubucket.SeqBucketDB = buckets.db

  def printPathsByPartition() {
    var kms = new KmerSpace()
    Stats.begin()
    val graph = buckets.makeGraph(kms)
    Stats.end("Construct graph")
    kms = null //Recover memory

    Stats.begin()
    val partBuild = new PartitionBuilder(graph)
    var parts = partBuild.partition(partitionSize)
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

    parts = partBuild.collapse(partitionSize, parts)
    printPaths(graph, parts)
  }

  def printPaths(graph: Graph[MarkerSet], parts: List[List[MarkerSet]]) {
    val pp = new PathPrinter("hypercut.fasta", k, printReasons)

    var pcount = 0

    @volatile
    var lengths = List[Int]()
    val minLength = k + 10
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
        if (s.length >= minPrintLength) {
          pp.printSequence(s"hypercut-part$pcount", s)
        }
      }

      if (outputPartitionGraphs) {
        GraphViz.writeUndirected(pathGraph, s"part$pcount.dot", (n: PathNode) => n.seq)
      }
    }
    pp.close()
    Stats.end("Find and print sequences")
    new Histogram(lengths, 20).print("Contig length")
  }
}