package dbpart.graph

import dbpart.HasID
import scala.reflect._
import dbpart.IDSpace
import friedrich.graph.Graph

trait ArrayBackedGraph[N <: HasID] extends Graph[N] {
  def nodesArr: Array[N]
  implicit def elemTag: ClassTag[N]

  val idSpace = new IDSpace(nodesArr)

  def nodes = nodesArr.iterator

  def addNode(node: N) {
    throw new Exception("Adding nodes to the array backed graph is not allowed")
  }
}

/**
 * A fast graph builder.
 * It is assumed that each edge is added only once, so no deduplication is performed.
 * This implementation is based on arrays and exploits the ID number assignment of
 * each node. All nodes must be available at the time of construction, and the
 * node set cannot change subsequently.
 *
 * This implementation stores only forward edges.
 */
class ArrayListGraph[N <: HasID : ClassTag](val nodesArr: Array[N])
  extends ArrayBackedGraph[N] with Serializable {

  def elemTag = classTag[N]

  type EdgeList = List[N]

  protected final val adjList = Array.fill(nodesArr.length)(emptyEdgeList)

  override def numEdges: Int = adjList.iterator.map(_.size).sum

  def emptyEdgeList: EdgeList = Nil

  def edgesFrom(from: N): List[N] = adjList(from.id)

  def edgesTo(from: N): List[N] = ???

  def edges: Iterator[(N, N)] = nodes.flatMap(n => {
    edgesFrom(n).map(x => (n, x)) ++ edgesTo(n).map(x => (x, n))
  })

  def addEdge(from: N, to: N) {
    addForward(from, to)
  }

  protected def addForward(from: N, to: N) {
    adjList(from.id) ::= to
  }
}

/**
 * An array-based graph builder that stores both forward and reverse edges.
 */
class DoubleArrayListGraph[N <: HasID : ClassTag](nodesArr: Array[N])
  extends ArrayListGraph[N](nodesArr) with Serializable {
  protected final val revAdjList = Array.fill(nodesArr.length)(emptyEdgeList)

  override def addEdge(from: N, to: N) {
    super.addEdge(from, to)
    addReverse(to, from)
  }

  override def edgesTo(from: N): List[N] = revAdjList(from.id)

  protected def addReverse(from: N, to: N) {
    revAdjList(from.id) ::= to
  }
}