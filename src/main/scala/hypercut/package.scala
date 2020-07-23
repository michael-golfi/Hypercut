import hypercut.hash.MotifSet

package object hypercut {

  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String

  type SequenceID = String

  /**
   * Abundance counts for k-mers.
   */
  type Abundance = Short

  type CompactEdge = (Array[Byte], Array[Byte])

  type ExpandedEdge = (MotifSet, MotifSet)

}
