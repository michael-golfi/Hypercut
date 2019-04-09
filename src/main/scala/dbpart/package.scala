import dbpart.graph.MacroNode
import dbpart.hash.MarkerSet

package object dbpart {
  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String

  /**
   * Type of edges between buckets in the macro graph.
   */
  type MacroEdge = (MacroNode, MacroNode)

  type CompactEdge = (Array[Byte], Array[Byte])

}
