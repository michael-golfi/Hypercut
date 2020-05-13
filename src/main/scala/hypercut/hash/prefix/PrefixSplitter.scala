package hypercut.hash.prefix

import hypercut.hash.ReadSplitter
import hypercut.shortread.Read
import miniasm.genome.bpbuffer.BPBuffer

/**
 * Naive hash function that hashes each k-mer to its n-length prefix.
 * Expected to give bad results, used as a baseline.
 * @param prefixLength
 * @param k
 */
case class PrefixSplitter(prefixLength: Int, val k: Int) extends ReadSplitter[String] {
  /**
   * Split the read into superkmers overlapping by (k-1) bases.
   *
   * @param read
   * @return
   */
  override def split(read: String): Iterator[(String, String)] = {
    //TODO join kmers with identical prefixes
    Read.kmers(read, k).map(km => (km.substring(0, prefixLength), km))
  }

  /**
   * Convert a hashcode into a compact representation.
   *
   * @param hash
   * @return
   */
  override def compact(hash: String): Array[Byte] = {
    BPBuffer.wrap(hash).data
  }
}
