package dbpart.graph

import dbpart.HasID
import scala.reflect.ClassTag
import dbpart.IDSpace
import friedrich.graph.Graph

trait ArrayBackedGraph[N <: HasID with AnyRef] extends Graph[N] {
  def nodesArr: Array[N]
  implicit def classTag: ClassTag[N]

  val idSpace = new IDSpace(nodesArr)

  def nodes = nodesArr.iterator

  //Do not use - TODO
  def addNode(node: N) = ???
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
class ArrayListGraph[N <: HasID with AnyRef](val nodesArr: Array[N])(implicit val classTag: ClassTag[N])
  extends ArrayBackedGraph[N] {

  type EdgeList = List[N]

  protected final val adjList = Array.fill(nodesArr.length)(emptyEdgeList)

  override def numEdges: Int = adjList.iterator.map(_.size).sum

  def emptyEdgeList: EdgeList = Nil

  def edgesFrom(from: N): Seq[N] = adjList(from.id)

  def edgesTo(from: N): Seq[N] = ???

  def edges: Iterator[(N, N)] = nodes.flatMap(n => {
    edgesFrom(n).map(x => (n, x)) ++ edgesTo(n).map(x => (x, n))
  })

  def addEdge(from: N, to: N) {
    addForward(from, to)
//    addBackward(to, from)
  }

  protected def addForward(from: N, to: N) {
    adjList(from.id) ::= to
  }

}

/**
 * An array-based graph builder that stores both forward and reverse edges.
 */
class DoubleArrayListGraph[N <: AnyRef with HasID](nodesArr: Array[N])(implicit tag: ClassTag[N])
  extends ArrayListGraph[N](nodesArr) {
  protected final val revAdjList = Array.fill(nodesArr.length)(emptyEdgeList)

  override def addEdge(from: N, to: N) {
    super.addEdge(from, to)
    addBackward(to, from)
  }

  override def edgesTo(from: N): Seq[N] = revAdjList(from.id)

  protected def addBackward(from: N, to: N) {
    revAdjList(from.id) ::= to
  }
}