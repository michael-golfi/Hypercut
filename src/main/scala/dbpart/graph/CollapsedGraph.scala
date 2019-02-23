package dbpart.graph

import dbpart.FastAdjListGraph
import friedrich.graph.Graph

final class NodeGroup[A](val nodes: Seq[A])

object CollapsedGraph {
  
  type G[A] = NodeGroup[A]
  
  /**
   * Construct a collapsed graph by turning each partition into a node group,
   * giving each group the union of the corresponding edges in the underlying graph.
   * 
   * Not currently used.
   */
  def construct[A](partitions: Iterable[Iterable[A]],
      underlying: Graph[A]): Graph[G[A]] = {
    type Group = NodeGroup[A]
    
    val groups = partitions.map(p => new NodeGroup(p.toSeq))
    val lookup = Map() ++ groups.flatMap(g => g.nodes.map(_ -> g)) 
    
    val r = new FastAdjListGraph[G[A]]
    for (g <- groups) {
      r.addNode(g)
    }
    for (g <- groups) {
      val underNodes = g.nodes
      /*
       * Note: deduplication of edges here is expensive.
       * Given how edgesFrom works, is it necessary?
       */
      val underEdges = underNodes.flatMap(n => underlying.edgesFrom(n)).distinct
      val underEdgeGroups = underEdges.map(e => lookup(e)).distinct
      for (ueg <- underEdgeGroups) {
        r.uncheckedAddEdge(g, ueg)
      }
    }
    r
  }
}
