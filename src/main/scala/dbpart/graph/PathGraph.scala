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

    var hypotheticalEdges = 0

    // Counting hypothetical edges (between buckets) for testing purposes
    for {
      subpart <- part
      toInside = macroGraph.edgesFrom(subpart).iterator.filter(partSet.contains)
    } {
      hypotheticalEdges += toInside.size
    }

    var realEdges = Set[(MarkerSet, MarkerSet)]()

    /**
     * Constructing all the edges here can be somewhat expensive.
     */
    for {
      subpart <- part
      (overlap, fromSeqs) <- byEnd(subpart.packedString)
      toInside = macroGraph.edgesFrom(subpart).iterator.filter(partSet.contains)
      toBucket <- toInside
    } {
      val toSeqs = sorted(toBucket.packedString).iterator.
        dropWhile(!_.seq.startsWith(overlap)).
        takeWhile(_.seq.startsWith(overlap))
      if (!toSeqs.isEmpty) {
        realEdges += ((subpart, toBucket))
      }
      for {
        from <- fromSeqs
        to <- toSeqs
      } {
        result.addEdge(from, to)
      }
    }
    println(s"Real/hypothetical edges ${realEdges.size}/$hypotheticalEdges")
  }

}