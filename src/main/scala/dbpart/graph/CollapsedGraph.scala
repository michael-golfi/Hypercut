package dbpart.graph

import dbpart.FastAdjListGraph
import friedrich.graph.Graph

case class NodeGroup[A](nodes: Seq[A])

object CollapsedGraph {
  
  type G[A] = NodeGroup[A]
  
  /**
   * Construct a collapsed graph by turning each partition into a node group,
   * giving each group the union of the corresponding edges in the underlying graph.
   */
  def construct[A](partitions: Iterable[Iterable[A]],
      underlying: Graph[A]): Graph[G[A]] = {
    type Group = NodeGroup[A]
    
    val groups = partitions.map(p => NodeGroup(p.toSeq))
    val lookup = Map() ++ groups.flatMap(g => g.nodes.map(_ -> g)) 
    
    val r = new FastAdjListGraph[G[A]]
    for (g <- groups) {
      r.addNode(g)
    }
    for (g <- groups) {
      val underNodes = g.nodes
      val underEdges = underNodes.flatMap(n => underlying.edgesFrom(n)).distinct
      val underEdgeGroups = underEdges.map(e => lookup(e)).distinct
      for (ueg <- underEdgeGroups) {
        r.uncheckedAddEdge(g, ueg)
      }
    }
    r
  }
}
