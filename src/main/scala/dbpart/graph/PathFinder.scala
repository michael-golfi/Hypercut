package dbpart.graph

import scala.annotation.tailrec
import friedrich.graph.Graph
import dbpart.Contig
import dbpart.PathPrinter

/**
 * Finds and prints branch-free paths.
 */
class PathFinder(outputFasta: String, k: Int, printReasons: Boolean)
  extends PathPrinter(outputFasta, printReasons) {
  type N = KmerNode

  def findSequences(graph: Graph[N]) = {
    //Reset flag
    for (n <- graph.nodes) {
      n.seen = false
    }
    val r = for {
      n <- graph.nodes
      if !n.seen
      if !n.noise
      p1 = extendForward(graph, n)
      p2 = extendBackward(graph, n)
      p = Contig(p2._1.map(_.seq) ::: p1._1.map(_.seq).reverse.tail, k,
        p2._2, p1._2)
    } yield p
    r.toList
  }

  private def edgeStopReason(edges: Iterable[N]) = {
    if (edges.size > 1) "Branch"
    else if (edges.size == 0) "Terminus"
    else "Loop?"
  }

  /**
   * Extend an unambiguous path forward.
   * "from" is assumed to participate in the path.
   * Also return the reason for stopping.
   */
  @tailrec
  final def extendForward(graph: Graph[N], from: N, acc: List[N] = Nil): (List[N], String) = {
    from.seen = true
    if (from.boundary) {
      //Boundaries have hidden branches
      //TODO distinguish the different boundary cases here?
      return (from :: acc, s"Boundary_${from.boundaryPartition.get}")
    }
    val ef = graph.edgesFrom(from).filter(! _.noise)
    if (ef.size > 1 || ef.size == 0) {
      (from :: acc, edgeStopReason(ef))
    } else {
      val candidate = ef.head
      val et = graph.edgesTo(candidate).filter(! _.noise)
      if (et.size > 1 || candidate.seen) {
        (from :: acc, edgeStopReason(et))
      } else {
        //'from' should be the only node linking to it
        extendForward(graph, candidate, from :: acc)
      }
    }
  }

  @tailrec
  final def extendBackward(graph: Graph[N], from: N, acc: List[N] = Nil): (List[N], String) = {
    from.seen = true
    if (from.boundary) {
      //Boundaries have hidden branches
      //TODO distinguish the different boundary cases here?
      return (from :: acc, s"Boundary_${from.boundaryPartition.get}")
    }
    val et = graph.edgesTo(from).filter(! _.noise)
    if (et.size > 1 || et.size == 0) {
      (from :: acc, edgeStopReason(et))
    } else {
      val candidate = et.head
      val ef = graph.edgesFrom(candidate).filter(! _.noise)
      if (ef.size > 1 || candidate.seen) {
        (from :: acc, edgeStopReason(ef))
      } else {
        //'from' should be the only node linking to it
        extendBackward(graph, candidate, from :: acc)
      }
    }
  }
}