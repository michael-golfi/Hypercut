package hypercut.shortread

import miniasm.genome.util.DNAHelpers
import scala.annotation.tailrec

object Read {
  def numKmers(data: String, k: Int) = data.length - (k-1)

  /**
   * Extract all k-mers from a read.
   */
  def kmers(data: String, k: Int): Iterator[String] = {
    new Iterator[String] {
      val len = data.length()
      var at = 0
      def hasNext = at <= len - k

      def next = {
        val r = data.substring(at, at + k)
        at += 1
        r
      }
    }
  }

  /**
   * Traverse a sequence of k-mers and join together adjacent paris that overlap by k-1
   * Result will be joined sequences in reverse order.
   */
  @tailrec
  def flattenKmers(kmers: List[String], k: Int, acc: List[String]): List[String] = {
    (kmers, acc) match {
      case (km :: ks, a :: as) =>
        if (DNAHelpers.kmerPrefix(km, k) == DNAHelpers.kmerSuffix(a, k)) {
          flattenKmers(ks, k, (a + km.takeRight(1)) :: as)
        } else {
          flattenKmers(ks, k, km :: a :: as)
        }
      case (km :: ks, Nil) => flattenKmers(ks, k, km :: Nil)
      case _ => acc
    }
  }
}