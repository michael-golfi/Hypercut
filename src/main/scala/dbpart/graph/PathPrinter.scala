package dbpart.graph

import dbpart._

import java.io.PrintWriter
import friedrich.graph.Graph
import scala.annotation.tailrec

case class Contig(nodes: List[PathNode], k: Int) {
  lazy val length = seq.length

  lazy val seq = {
    if (nodes.isEmpty) {
      ""
    } else {
      val sb = new StringBuilder
      sb.append(nodes.head.seq)
      for (s <- nodes.tail) {
        sb.append(s.seq.substring(k-1))
      }
      sb.result()
    }
  }
}

final class PathPrinter(outputFasta: String, k: Int) {
  var count: Int = 0
  val w: java.io.PrintWriter = new PrintWriter(outputFasta)

  def close() {
    w.close()
  }

  def printSequence(prefix: String, seq: Contig) {
    printSequence(prefix, seq.seq)
  }

  def printSequence(prefix: String, seq: NTSeq) = synchronized {
    count += 1
    w.println(s">$prefix-seq$count-${seq.size}bp")
    w.println(seq)
  }

  def findSequences(graph: Graph[PathNode]) = {
    //Reset flag
    for (n <- graph.nodes) {
      n.seen = false
    }
    val r = for {
      n <- graph.nodes
      if !n.seen
      p1 = extendForward(graph, n);
      p2 = extendBackward(graph, n)
      p = Contig(p2 ::: p1.reverse.tail, k)
    } yield p
    r.toList
  }

  /**
   * Extend an unambiguous path forward.
   * "from" is assumed to participate in the path.
   */
  @tailrec
  def extendForward(graph: Graph[PathNode], from: PathNode, acc: List[PathNode] = Nil): List[PathNode] = {
    from.seen = true
    val ef = graph.edgesFrom(from)
    if (ef.size > 1 || ef.size == 0) {
      from :: acc
    } else {
      val candidate = ef.head
      val et = graph.edgesTo(candidate)
      if (et.size > 1 || candidate.seen) {
        from :: acc
      } else {
        //'from' should be the only node linking to it
        extendForward(graph, candidate, from :: acc)
      }
    }
  }

  @tailrec
  def extendBackward(graph: Graph[PathNode], from: PathNode, acc: List[PathNode] = Nil): List[PathNode] = {
    from.seen = true
    val et = graph.edgesTo(from)
    if (et.size > 1 || et.size == 0) {
      from :: acc
    } else {
      val candidate = et.head
      val ef = graph.edgesFrom(candidate)
      if (ef.size > 1 || candidate.seen) {
        from :: acc
      } else {
        //'from' should be the only node linking to it
        extendBackward(graph, candidate, from :: acc)
      }
    }
  }

}