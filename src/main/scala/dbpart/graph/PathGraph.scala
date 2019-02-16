package dbpart.graph

import dbpart._
import dbpart.ubucket.SeqBucketDB
import dbpart.MarkerSet
import friedrich.graph.Graph
import dbpart.FastAdjListGraph
import scala.annotation.tailrec
import friedrich.util.formats.GraphViz

/**
 * Builds a graph where every node is an unambiguous sequence path.
 */
class PathGraphBuilder(pathdb: SeqBucketDB, partitions: Iterable[Iterable[MarkerSet]],
                       macroGraph: Graph[MarkerSet]) {

  val k: Int = pathdb.k
  val result: Graph[PathNode] = new DoublyLinkedGraph[PathNode]

  for (p <- partitions) {
    addPartition(p)
  }

  /**
   * Add marker set buckets to the path graph.
   * The buckets passed in together are assumed to potentially contain adjacent sequences,
   * and will be scanned for such. If they are found, an edge between two sequences is added
   * to the graph being constructed.
   */
  def addPartition(part: Iterable[MarkerSet]) {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")

    //For manual inspection of local features
    val localGraph = new DoublyLinkedGraph[PathNode]

    val partSet = part.toSet
    val sequences = Map() ++ pathdb.getBulk(part.map(_.packedString)).
      map(x => (x._1 -> x._2.sequences.map(new PathNode(_))))

    for (
      subpart <- part;
      subpartSeqs = sequences(subpart.packedString);
      s <- subpartSeqs
    ) {
      result.addNode(s)
    }

    var sortedCache = Map[String, List[PathNode]]()

    def sorted(bucket: String): List[PathNode] = {
      sortedCache.get(bucket) match {
        case Some(s) => s
        case _ =>
          sortedCache += (bucket -> sequences(bucket).toList.sortBy(_.seq))
          sortedCache(bucket)
      }
    }

    val byEnd = sequences.map(x => (x._1 -> x._2.groupBy(_.seq.takeRight(k - 1))))
//    val sorted = sequences.mapValues(_.toSeq.sorted)

    /**
     * Constructing all the edges here is somewhat expensive. Needs to be tuned.
     */
    for {
      subpart <- part
      toInside = macroGraph.edgesFrom(subpart).iterator.filter(partSet.contains)
      (overlap, ss) <- byEnd(subpart.packedString)
      toBucket <- toInside
      toSeqs = sorted(toBucket.packedString).iterator.
        dropWhile(! _.seq.startsWith(overlap)).
        takeWhile(_.seq.startsWith(overlap))
      to <- toSeqs
      s <- ss
    } {
      result.addEdge(s, to)
    }
  }

}