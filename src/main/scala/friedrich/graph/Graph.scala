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

  def containsNode(node: N)

  def numEdges: Int = edges.size
  def numNodes: Int = nodes.size

  def edgesFrom(from: N): Iterable[N]
  def edgesTo(to: N): Iterable[N]
  /**
   * Both to- and from-nodes of the given node
   */
  def edges(node: N): Iterable[N] = edgesFrom(node) ++ edgesTo(node)
  /**
   * All edges in the graph
   */
  def edges(): Iterator[(N, N)]

  def hasEdge(from: N, to: N): Boolean = edgesFrom(from).toSet.contains(to)
  def hasNode(node: N): Boolean = nodes.contains(node)
  
  /**
   * Users should call this method to add a node.
   */
  def addNode(node: N): Unit = {
    if (!hasNode(node)) {
      implAddNode(node)
    }
  }

  def removeNode(node: N): Unit = {}
  def removeEdges(node: N): Unit = {}

  /**
   * Users should call this method to add an edge.
   */
  def addEdge(from: N, to: N): Unit = {
    assert(hasNode(from))
    assert(hasNode(to))
    implAddEdge(from, to)
  }

  def uncheckedAddEdge(from: N, to: N): Unit = implAddEdge(from, to)

  /**
   * This method should be overridden to implement the operation of adding a node.
   * The graph must tolerate the same node being added multiple times.
   */
  protected def implAddNode(node: N)

  /**
   * This method should be overridden to implement the operation of adding an edge.
   * The graph must tolerate the same edge being added multiple times.
   */
  protected def implAddEdge(from: N, to: N)

  /**
   * Print a textual representation of the graph.
   * For debug purposes.
   */
  def printBare() = {
    for (n <- nodes) {
      println(n)
      print(n + " to: ")
      for (e <- edgesFrom(n)) {
        print(e + ", ")
      }
      print(" from: ")
      for (e <- edgesTo(n)) {
        print(e + ", ")
      }
      println("")
    }
  }

  def stats() {
    println(s"$numNodes nodes")    
    println(s"$numEdges edges")    
  }
}

/**
 * Trait for a graph that stores nodes in a mutable set.
 */
trait SetBackedGraph[N <: AnyRef] extends Graph[N] {
  override def addNode(node: N): Unit = {    
    implAddNode(node)    
  }
  
  final def implAddNode(node: N) {
    nodesv.add(node)
  }
  
  final def containsNode(node: N) = nodesv.contains(node)

  override def removeNode(node: N) {
    super.removeNode(node)
    nodesv.remove(node)
    removeEdges(node)
  }

  final def nodes(): Iterator[N] = nodesv.iterator

  private val nodesv = new mutable.HashSet[N]

  final override def hasNode(node: N) = nodesv.contains(node)
}

