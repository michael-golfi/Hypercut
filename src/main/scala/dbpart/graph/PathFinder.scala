package dbpart.graph

import scala.annotation.tailrec
import friedrich.graph.Graph
import dbpart.Contig
import dbpart.PathPrinter

/**
 * Finds and prints branch-free paths.
 */
class PathFinder(k: Int) {
  type N = KmerNode

  def findSequences(graph: Graph[N]): List[Contig] = {
    //Reset flag
    for (n <- graph.nodes) {
      n.seen = false
    }
    val r = for {
      n <- graph.nodes
      if !n.seen
      if !n.noise
      if !n.boundary
      p1 = extendForward(graph, n)
      p2 = extendBackward(graph, n)
      p = Contig(p2._1.map(_.seq) ::: p1._1.map(_.seq).reverse.tail, k,
        p2._2, p1._2)
    } yield p
    r.toList
  }

  final def atLeastTwo(data: List[Any]): Boolean = data match {
    case x :: y :: _ => true
    case _ => false
  }

  private def edgeStopReason(edges: Iterable[N]) = {
    if (edges.size > 1) "Branch"
    else if (edges.isEmpty) "Terminus"
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
    from.boundaryPartition match {
      case Some(bp) =>
        //Boundaries have hidden branches
        //TODO distinguish the different boundary cases here?
        return (from :: acc, s"Boundary_${bp}")
      case _ =>
    }

    val ef = graph.edgesFrom(from).iterator.filter(! _.noise).take(2).toList
    if (atLeastTwo(ef) || ef.isEmpty) {
      (from :: acc, edgeStopReason(ef))
    } else {
      val candidate = ef.head
      val et = graph.edgesTo(candidate).iterator.filter(! _.noise).take(2).toList
      if (atLeastTwo(et) || candidate.seen) {
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
    from.boundaryPartition match {
      case Some(bp) =>
        //Boundaries have hidden branches
        //TODO distinguish the different boundary cases here?
        return (from :: acc, s"Boundary_${bp}")
      case _ =>
    }

    val et = graph.edgesTo(from).iterator.filter(! _.noise).take(2).toList
    if (atLeastTwo(et) || et.isEmpty) {
      (from :: acc, edgeStopReason(et))
    } else {
      val candidate = et.head
      val ef = graph.edgesFrom(candidate).iterator.filter(! _.noise).take(2).toList
      if (atLeastTwo(ef) || candidate.seen) {
        (from :: acc, edgeStopReason(ef))
      } else {
        //'from' should be the only node linking to it
        extendBackward(graph, candidate, from :: acc)
      }
    }
  }
}