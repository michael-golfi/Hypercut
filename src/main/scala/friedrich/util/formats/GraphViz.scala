/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package friedrich.util.formats

import friedrich.graph._
import java.io._

object GraphViz {
  
  /**
   * Write a graph to a .dot file (graphviz format)
   */
  def write[T <: AnyRef](g: Graph[T], file: String,
      edgeFormat: T => String) = {
    val w = new BufferedWriter(new FileWriter(file))
    w.write("graph mygraph {\n")
    for (n <- g.nodes; e <- g.edgesFrom(n)) {    
      w.write("  \"" + edgeFormat(n) + "\" -> \"" + edgeFormat(e) + "\"\n")
    }
    w.write("}\n")
    w.close()
  }
}