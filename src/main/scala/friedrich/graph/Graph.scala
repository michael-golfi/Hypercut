/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package friedrich.graph

import scala.collection.immutable.{ HashMap, HashSet }
import scala.collection.{ mutable, Set }

/**
 * Base trait for graph implementations.
 * A graph is a set of nodes and edges.
 */
trait Graph[N] {

  def nodes: Iterator[N]

  def numEdges: Int = edges.size
  def numNodes: Int = nodes.size

  def edgesFrom(from: N): List[N]
  def edgesTo(to: N): List[N]

  def atLeastTwo(data: List[Any]): Boolean = data match {
    case x :: y :: _ => true
    case _ => false
  }

  def isBranchOut(e: N) = atLeastTwo(edgesFrom(e))
  def isBranchIn(e: N) = atLeastTwo(edgesTo(e))

  def degree(from: N): Int = fromDegree(from) + toDegree(from)
  def fromDegree(from: N): Int = edgesFrom(from).size
  def toDegree(to: N): Int = edgesTo(to).size

  /**
   * Both to- and from-nodes of the given node
   */
  def edges(node: N): Iterator[N] = edgesFrom(node).iterator ++ edgesTo(node).iterator

  /**
   * All edges in the graph
   */
  def edges(): Iterator[(N, N)]

  def hasEdge(from: N, to: N): Boolean = edgesFrom(from).toSet.contains(to)

  /**
   * Users should call this method to add a node.
   * The same node may not be added twice.
   */
  def addNode(node: N): Unit

  /**
   * Users should call this method to add an edge.
   */
  def addEdge(from: N, to: N): Unit

  /**
   * Print a textual representation of the graph.
   * For debug purposes.
   */
  def printBare(): Unit = for (n <- nodes) {
    println(n)
    println("  Out: " + edgesFrom(n).mkString(","))
    println("  In: " + edgesTo(n).mkString(","))
  }

  def stats() {
    println(s"$numNodes nodes")
    println(s"$numEdges edges")
  }
}
