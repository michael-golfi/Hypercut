import hypercut.hash.MotifSet

package object hypercut {

  /**
   * Type of nucleotide sequences.
   */
  type NTSeq = String
  
  type Kmer = NTSeq
  
  type Segment = NTSeq

  /**
   * Abundance counts for k-mers.
   */
  type Abundance = Short

  type CompactEdge = (Array[Byte], Array[Byte])

  type ExpandedEdge = (MotifSet, MotifSet)

}