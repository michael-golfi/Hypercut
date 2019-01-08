package dbpart

import scala.collection.mutable.Map

import friedrich.graph.SetBackedGraph
import scala.collection.mutable.ArrayBuffer

/**
 * A graph that stores nodes in sets and edges in vectors.
 * It is assumed that each edge is added only once, so no
 * deduplication is performed.
 * The underlying implementation is based on mutable data structures for fast construction.
 * 
 * Currently this graph only stores forward edges.
 * It is trivial to change implAddEdge to also store reverse edges.
 */
class FastAdjListGraph[N <: AnyRef] extends SetBackedGraph[N] {
  //If the type of EdgeList is changed, addForward and addBackward may also need to be
  //updated accordingly for efficiency. e.g. prepend is inefficient for ArrayBuffer
  type EdgeList = ArrayBuffer[N]
  
  private val adjList = Map[N, EdgeList]()
  private val revAdjList = Map[N, EdgeList]()

  override def numEdges = nodes.foldLeft(0)(_ + edgesFrom(_).size)

  def emptyEdgeList: EdgeList = ArrayBuffer()
  
  def edgesFrom(from: N): Seq[N] = adjList.getOrElse(from, emptyEdgeList)

  def edgesTo(from: N): Seq[N] = revAdjList.getOrElse(from, emptyEdgeList)
  
  def edges: Iterator[(N, N)] = nodes.flatMap(n => {
    edgesFrom(n).map(x => (n, x)) ++ edgesTo(n).map(x => (x, n))
  })

  override def implAddEdge(from: N, to: N) {
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
    ol += to
  }
  
  protected def addBackward(from: N, to: N) {
     val ol = revAdjList.get(from) match {
      case Some(list) => list
      case None => 
        revAdjList += (from -> emptyEdgeList)
        revAdjList(from)
    }        
    ol += to
  }
}