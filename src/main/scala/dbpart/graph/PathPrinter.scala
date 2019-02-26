package dbpart.graph

import dbpart._

import java.io.PrintWriter
import friedrich.graph.Graph
import scala.annotation.tailrec

case class Contig(nodes: List[KmerNode], k: Int,
  stopReasonStart: String, stopReasonEnd: String) {
  lazy val length = seq.length

  lazy val seq = {
    if (nodes.isEmpty) {
      ""
    } else {
      val sb = new StringBuilder
      sb.append(nodes.head.seq)
      for (s <- nodes.tail) {
        sb.append(s.seq.charAt(k-1))
      }
      sb.result()
    }
  }
}

final class PathPrinter(outputFasta: String, k: Int, printReasons: Boolean) {
  type N = KmerNode

  var count: Int = 0
  val w: java.io.PrintWriter = new PrintWriter(outputFasta)
  val statsW = if (printReasons) Some(new PrintWriter(outputFasta + "_stats.txt")) else None

  def close() {
    w.close()
    for (w2 <- statsW) w2.close
  }

  def printSequence(prefix: String, seq: Contig) {
    val rsn = if (printReasons) Some(seq.stopReasonStart + " " + seq.stopReasonEnd) else None
    printSequence(prefix, seq.seq, rsn)
  }

  def printSequence(prefix: String, seq: NTSeq, reasons: Option[String]) = synchronized {
    count += 1
    val idLine = s">$prefix-seq$count-${seq.size}bp"
    w.println(idLine)
    w.println(seq)
    (reasons, statsW) match {
      case (Some(r), Some(w)) =>
        w.println(idLine)
        w.println(r)
      case _ =>
    }    
  }

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
      p = Contig(p2._1 ::: p1._1.reverse.tail, k,
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
  def extendForward(graph: Graph[N], from: N, acc: List[N] = Nil): (List[N], String) = {
    from.seen = true
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
  def extendBackward(graph: Graph[N], from: N, acc: List[N] = Nil): (List[N], String) = {
    from.seen = true
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