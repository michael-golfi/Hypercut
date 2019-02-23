package object dbpart {
  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String
  
  /**
   * Type of edges between buckets in the macro graph.
   */
  type MacroEdge = (MarkerSet, MarkerSet)
}
