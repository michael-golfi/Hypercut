package dbpart.graph

import dbpart._
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

  val k: Int = pathdb.k
  val result: Graph[PathNode] = new DoublyLinkedGraph[PathNode]

  for (p <- partitions) {
    addPartition(p)
  }

  def addPartition(part: Iterable[MarkerSet]) {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")

    val partSet = part.toSet
    val sequences = Map() ++ pathdb.getBulk(part.map(_.packedString)).
      map(x => (x._1 -> x._2.map(new PathNode(_))))

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

    val byEnd = sequences.map(x => (x._1 -> x._2.groupBy(_.seq.substring(1, k))))
//    val sorted = sequences.mapValues(_.toSeq.sorted)

    /**
     * Constructing all the edges here is somewhat expensive. Needs to be tuned.
     */
    for {
      subpart <- part
      toInside = macroGraph.edgesFrom(subpart).filter(partSet.contains)
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