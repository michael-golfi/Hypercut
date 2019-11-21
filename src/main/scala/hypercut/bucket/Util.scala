package hypercut.bucket

import com.github.pathikrit.scalgos.DisjointSet
import hypercut.shortread.Read
import miniasm.genome.util.DNAHelpers

object Util {

  /**
   * Given a collection and a function that produces multiple keys from each item,
   * produces a partitioning such that items are in the same partition iff they
   * share at least one key.
   * Items are expected to be distinct.
   */
  def partitionByKeys[A, K](items: List[A], keys: A => Iterable[K]): List[List[A]] = {
    val disjoint = DisjointSet.empty[K, A]
    for {
      i <- items
      ks = keys(i)
      head <- ks.headOption
    } {
      if (!disjoint.contains(head)) {
        disjoint += head
      }
      disjoint.assignData(head, i)
      for {
        k <- ks
        if !disjoint.contains(k)
      } {
        disjoint += k
      }
    }

    for {
      i <- items
      ks = keys(i)
      head <- ks.headOption
      k <- ks
    } {
      disjoint.union(head, k)
    }
    val partitioned = disjoint.setAnnotations.map(_.toList)
    partitioned.toList
  }

  def prefixesAndSuffixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      //Number of (k-1)-mers
      val num = s.length() - (k - 2)
      Read.kmers(s, k - 1).slice(1, num - 2 + 1)
    })

  def purePrefixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      if (s.length >= k - 1) {
        Some(DNAHelpers.kmerPrefix(s, k))
      } else {
        None
      }
    })

  def pureSuffixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      if (s.length >= k - 1) {
        Some(DNAHelpers.kmerSuffix(s, k))
      } else {
        None
      }
    })

  def suffixes(ss: Iterable[String], k: Int) =
    pureSuffixes(ss.iterator, k) ++ prefixesAndSuffixes(ss.iterator, k)

  def prefixes(ss: Iterable[String], k: Int) =
    purePrefixes(ss.iterator, k) ++ prefixesAndSuffixes(ss.iterator, k)

  type IdChunk = (Array[String], Long)
}