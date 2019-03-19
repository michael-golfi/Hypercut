package dbpart.graph

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import friedrich.graph.Graph

/**
 * A graph builder based on maps, which also stores all nodes in a set.
 * It is assumed that each edge is added only once, so no deduplication is performed.
 * The underlying implementation is based on mutable data structures for fast construction.
 *
 * This implementation stores only forward edges.
 */
class MapListGraph[N <: AnyRef] extends Graph[N] {

  type EdgeList = List[N]

  protected final val adjList = Map[N, EdgeList]()

  var nodeList = List[N]()
  def nodes = nodeList.iterator

  override def numEdges: Int = nodes.foldLeft(0)(_ + edgesFrom(_).size)

  def emptyEdgeList: EdgeList = Nil

  def edgesFrom(from: N): Seq[N] = adjList.getOrElse(from, emptyEdgeList)

  def edgesTo(from: N): Seq[N] = ???

  def edges: Iterator[(N, N)] = nodes.flatMap(n => {
    edgesFrom(n).map(x => (n, x)) ++ edgesTo(n).map(x => (x, n))
  })

  def addNode(n: N) {
    nodeList ::= n
  }

  override def addEdge(from: N, to: N) {
    addForward(from, to)
//    addBackward(to, from)
  }

  protected def addForward(from: N, to: N) {
    val ol = adjList.get(from) match {
      case Some(list) => list
      case None =>
        adjList += (from -> emptyEdgeList)
        adjList(from)
    }
    adjList += (from -> (to :: ol))
  }
}

/**
 * A graph builder based on maps that stores both forward and reverse edges.
 */
class DoubleMapListGraph[N <: AnyRef] extends MapListGraph[N] {
  protected final val revAdjList = Map[N, EdgeList]()

  override def addEdge(from: N, to: N) {
    super.addEdge(from, to)
    addBackward(to, from)
  }

  override def edgesTo(from: N): Seq[N] = revAdjList.getOrElse(from, emptyEdgeList)

  protected def addBackward(from: N, to: N) {
     val ol = revAdjList.get(from) match {
      case Some(list) => list
      case None =>
        revAdjList += (from -> emptyEdgeList)
        revAdjList(from)
    }
    revAdjList += (from -> (to :: ol))
  }
}