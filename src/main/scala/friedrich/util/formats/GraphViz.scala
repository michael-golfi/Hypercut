/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package friedrich.util.formats

import friedrich.graph._
import java.io._

/**
 * Utility methods for exporting Graphs to .dot graphviz files.
 */
object GraphViz {

  private def begin(file: String, headerLine: String) = {
    val w = new PrintWriter(new FileWriter(file))
    w.println(headerLine)
    w
  }

  private def end(w: PrintWriter) {
    w.println("}")
    w.close()
  }

  /**
   * Write an undirected graph to a .dot file (graphviz format)
   */
  def writeUndirected[T <: AnyRef](g: Graph[T], file: String,
      nodeFormat: T => String): Unit = {
    val w = begin(file, "digraph mygraph {")
    for (n <- g.nodes; e <- g.edgesFrom(n)) {
      w.println("  \"" + nodeFormat(n) + "\" -> \"" + nodeFormat(e) + "\"")
    }
    end(w)
  }

  /**
   * Write a directed graph to a .dot file (graphviz format)
   */
  def writeDirected[T <: AnyRef](g: Graph[T], file: String,
      nodeFormat: T => String): Unit = {
    val w = begin(file, "digraph mygraph {")
    for (e <- g.edges) {
      w.println("  \"" + nodeFormat(e._1) + "\" -> \"" + nodeFormat(e._2) + "\"")
    }
    end(w)
  }

  def write[T <: AnyRef](g: Graph[T], file: String): Unit = writeDirected(g, file, (x: T) => x.toString)
}