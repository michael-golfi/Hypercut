package dbpart

import friedrich.graph.AdjListGraph
import friedrich.graph.SetBackedGraph

import scala.collection.mutable.Map
import friedrich.graph.Graph

/**
 * A graph that stores nodes in sets and edges in vectors
 * It is assumed that each edge is added only once, so no
 * deduplication is performed.
 */
class FastAdjListGraph[N <: AnyRef] extends SetBackedGraph[N] {
  private val adjList = Map[N, Vector[N]]()
  private val revAdjList = Map[N, Vector[N]]()

  override def numEdges = nodes.foldLeft(0)(_ + edgesFrom(_).size)

  def edgesFrom(from: N): Seq[N] = adjList.getOrElse(from, Seq())

  def edgesTo(from: N): Seq[N] = revAdjList.getOrElse(from, Seq())
  
  def edges: Iterator[(N, N)] = nodes.flatMap(n => {
    edgesFrom(n).map(x => (n, x)) ++ edgesTo(n).map(x => (x, n))
  })

  override def implAddEdge(from: N, to: N) {
    addForward(from, to)
    addBackward(to, from)
  }

  protected def addForward(from: N, to: N) {
    var ol = adjList.getOrElse(from, Vector())
    ol :+= to
    adjList += (from -> ol)
  }
  
  protected def addBackward(from: N, to: N) {
    var ol = revAdjList.getOrElse(from, Vector())
    ol :+= to
    revAdjList += (from -> ol)
  }
}