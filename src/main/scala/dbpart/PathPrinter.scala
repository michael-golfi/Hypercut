package dbpart

import dbpart._
import java.io.PrintWriter

case class Contig(nodes: List[String], k: Int,
  stopReasonStart: String, stopReasonEnd: String) {
  lazy val length = seq.length

  lazy val seq = {
    if (nodes.isEmpty) {
      ""
    } else {
      val sb = new StringBuilder
      sb.append(nodes.head.seq)
      for (s <- nodes.tail) {
        sb.append(s.charAt(k-1))
      }
      sb.result()
    }
  }
}

class PathPrinter(outputFasta: String, printReasons: Boolean) {

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

}