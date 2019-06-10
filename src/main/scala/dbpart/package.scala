import dbpart.graph.MacroNode
import dbpart.hash.MarkerSet

package object dbpart {
  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String

  /**
   * Abundance counts for k-mers.
   */
  type Abundance = Short
  
  /**
   * Type of edges between buckets in the macro graph.
   */
  type MacroEdge = (MacroNode, MacroNode)

  type CompactEdge = (Array[Byte], Array[Byte])

  type ExpandedEdge = (MarkerSet, MarkerSet)
  
}
