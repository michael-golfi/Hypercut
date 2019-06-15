package dbpart.graph

import dbpart.PartitionBuilder
import dbpart.SeqPrintBuckets
import dbpart.Stats
import friedrich.graph.Graph
import friedrich.util.Histogram
import friedrich.util.formats.GraphViz

/**
 * Encapsulates the operations of building a macro graph, partitioning it
 * into partitions of a given maximum size, and finding and printing contigs
 * from each partition.
 */
class PathExtraction(buckets: SeqPrintBuckets,
  partitionSize: Int, minPrintLength: Int, outputPartitionGraphs: Boolean,
  printReasons: Boolean) {
  val k: Int = buckets.k
  val db = buckets.db

  def printPathsByPartition() {
    Stats.begin()
    val graph = buckets.makeGraph()
    Stats.end("Construct graph")

    Stats.begin()
    val partBuild = new PartitionBuilder(graph, partitionSize)
    var parts = partBuild.partitions
    Stats.end("Partition graph")

    val hist = new Histogram(parts.map(_.size), 20)
    hist.print("Macro partition # marker sets")
    //        val numSeqs = parts.map(p => buckets.db.getBulk(p.map(_.packedString)).map(_._2.size).sum)
    //        new Histogram(numSeqs, 20).print("Macro partition # sequences")
    //        val seqLength = parts.map(p => buckets.db.getBulk(p.map(_.packedString)).map(_._2.map(_.length).sum).sum)
    //        new Histogram(seqLength, 20).print("Macro partition total sequence length")

    parts = partBuild.collapse(partitionSize, parts)
    printPaths(graph, parts)
  }

  def printPaths(graph: Graph[MacroNode], parts: List[List[MacroNode]]) {
    val pp = new PathFinder("hypercut.fasta", k, printReasons)

    var lengths = List[Int]()
    val minLength = k + 10
    Stats.begin()

    for ((p, pid) <- parts.zipWithIndex; n <- p) {
      n.partitionId = pid
    }

    for (p <- parts; pid = p.head.partitionId) {
      println(s"Process partition $pid/${parts.size}")

      val pathGraph = new MacroPathGraphBuilder(db, k, p, graph)(buckets.space).result
      println(s"Path graph ${pathGraph.numNodes} nodes (${pathGraph.nodes.filter(_.boundary).size} boundary) ${pathGraph.numEdges} edges")

//      val analyzer = new PathGraphAnalyzer(pathGraph, k)
//      analyzer.findBubbles()

      val ss = pp.findSequences(pathGraph)

      for (s <- ss) {
        if (s.length >= minLength) {
          synchronized {
            lengths ::= s.length
          }
        }
        if (s.length >= minPrintLength) {
          pp.printSequence(s"hypercut-part$pid", s)
        }
      }

      if (outputPartitionGraphs) {
        GraphViz.writeUndirected(pathGraph, s"part$pid.dot", (n: KmerNode) => n.seq)
      }
    }
    pp.close()
    Stats.end("Find and print sequences")
    new Histogram(lengths, 20).print("Contig length")
  }
}