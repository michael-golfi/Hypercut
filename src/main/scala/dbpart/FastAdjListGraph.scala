package dbpart

import scala.collection.mutable.Map

import friedrich.graph.SetBackedGraph
import scala.collection.mutable.ArrayBuffer

/**
 * A fast graph builder.
 * It is assumed that each edge is added only once, so no
 * deduplication is performed.
 * The underlying implementation is based on mutable data structures for fast construction.
 *
 * This implementation stores only forward edges.
 */
class FastAdjListGraph[N <: AnyRef] extends SetBackedGraph[N] {
  //If the type of EdgeList is changed, addForward and addBackward may also need to be
  //updated accordingly for efficiency. e.g. prepend is inefficient for ArrayBuffer
  type EdgeList = ArrayBuffer[N]

  protected final val adjList = Map[N, EdgeList]()

  override def numEdges: Int = adjList.valuesIterator.
    map(_.size).sum

  def emptyEdgeList: EdgeList = ArrayBuffer()

  def edgesFrom(from: N): Seq[N] = adjList.getOrElse(from, emptyEdgeList)

  def edgesTo(from: N): Seq[N] = ???
  
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

}

/**
 * A fast graph builder that also stores reverse edges.
 */
class DoublyLinkedGraph[N <: AnyRef] extends FastAdjListGraph[N] {
  protected final val revAdjList = Map[N, EdgeList]()

  override def implAddEdge(from: N, to: N) {
    super.implAddEdge(from, to)
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
    ol += to
  }
}
