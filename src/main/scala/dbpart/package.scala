import dbpart.graph.MacroNode

package object dbpart {
  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String

  type ExpandedEdge = (MarkerSet, MarkerSet)
  
  /**
   * Type of edges between buckets in the macro graph.
   */
  type MacroEdge = (MacroNode, MacroNode)

  type CompactEdge = (Array[Byte], Array[Byte])

}
