package dbpart.graph

import dbpart.ubucket.PathBucketDB
import dbpart.MarkerSet
import friedrich.graph.Graph
import dbpart.FastAdjListGraph
import scala.annotation.tailrec

/**
 * A graph where every node is an unambiguous path of maximal length.
 * Paths are constructed by joining together nodes within partitions, up to the partition limit.
 */
class PathGraphBuilder(pathdb: PathBucketDB, partitions: Iterable[Iterable[MarkerSet]],
                       macroGraph: Graph[MarkerSet]) {

  type NTSeq = String
  val k: Int = pathdb.k
  val result: dbpart.FastAdjListGraph[PathGraphBuilder.this.NTSeq] = new FastAdjListGraph[NTSeq]

  for (p <- partitions) {
    addPartition(p)
  }

  def addPartition(part: Iterable[MarkerSet]) {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")

    val partSet = part.toSet
    val sequences = pathdb.getBulk(part.map(_.packedString))

    for (
      subpart <- part;
      subpartSeqs = sequences(subpart.packedString);
      s <- subpartSeqs
    ) {
      result.addNode(s)
    }

    val byEnd = sequences.mapValues(_.groupBy(_.substring(1, k)))
    val byStart = sequences.mapValues(_.groupBy(_.substring(0, k - 1)))

    for {
      subpart <- part;
      toInside = macroGraph.edgesFrom(subpart).filter(partSet.contains);
      (group, ss) <- byEnd(subpart.packedString);
      toBucket <- toInside;
      toSeqs = sequences(toBucket.packedString);
      to <- byStart(toBucket.packedString).getOrElse(group, Seq());
      s <- ss
    } {
      result.addEdge(s, to)
    }
  }

}