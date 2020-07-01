package hypercut.bucket


import hypercut.hash.BucketId
import miniasm.genome.bpbuffer.BPBuffer

import scala.collection.mutable
import scala.util.Sorting

object SimpleBucket {
  def fromCountedSequences(id: BucketId,
                           data: Array[(Array[Int], Long)]): SimpleBucket = {
    val kmers = data.map(_._1)
    val abunds = data.map(x => {
      x._2.min(Integer.MAX_VALUE).toInt
    })
    SimpleBucket(id, kmers, abunds)
  }
}

/**
 * Maintains a set of k-mers with abundances in sorted order.
 * @param id
 * @param kmers
 * @param abundances
 */
final case class SimpleBucket(id: BucketId,
                        kmers: Array[Array[Int]],
                        abundances: Array[Int]) {

}

object ParentMap {
  val NONE = 0
}

/**
 * Maps each taxon to its parent.
 * @param parents
 */
final case class ParentMap(parents: Array[Int]) {
  import ParentMap._

  /**
   * Lowest common ancestor of two taxa.
   * @param tax1
   * @param tax2
   * @return
   */
  def lca(tax1: Int, tax2: Int): Int = {
    if (tax1 == NONE || tax2 == NONE) {
      return (if (tax2 == NONE) tax1 else tax2)
    }
    var a = tax1
    var path1 = mutable.Set.empty[Int]
    while (a != NONE) {
      path1 += a
      a = parents(a)
    }
    var b = tax2
    while (b != NONE) {
      if (path1.contains(b)) {
        return b
      }
      b = parents(b)
    }
    return NONE
  }

  import hypercut.spark.Counting._

  /**
   * From a series of sequences (where k-mers may be repeated) and where each sequence has a taxon tag,
   * produce an iterator where each k-mer appears only once and its taxon has been set to the lowest
   * common ancestor (LCA) of all its taxon tags.
   * @param segmentsTaxa
   * @param k
   * @return
   */
  def taxonTaggedFromSequences(segmentsTaxa: Iterable[(BPBuffer, Int)], k: Int): Iterator[(Array[Int], Int)] = {
    val byKmer = segmentsTaxa.iterator.flatMap(s =>
      s._1.kmersAsArrays(k.toShort).map(km => (km, s._2))
    ).toArray
    Sorting.quickSort(byKmer)

    new Iterator[(Array[Int], Int)] {
      var i = 0
      var remaining = byKmer
      val len = byKmer.length

      def hasNext = i < len

      def next = {
        val lastKmer = byKmer(i)._1
        var kmersLca = ParentMap.NONE
        while (i < len && java.util.Arrays.equals(byKmer(i)._1, lastKmer)) {
          kmersLca = lca(kmersLca, byKmer(i)._2)
          i += 1
        }

        (lastKmer, kmersLca)
      }
    }
  }
}

object TaxonBucket {
  def fromTaggedSequences(id: BucketId,
                           data: Array[(Array[Int], Int)]): TaxonBucket = {
    val kmers = data.map(_._1)
    val taxa = data.map(_._2)
    TaxonBucket(id, kmers, taxa)
  }
}

/**
 * A bucket where each k-mer is tagged with a taxon.
 * K-mers are sorted.
 * @param id
 * @param kmers
 * @param taxa
 */
final case class TaxonBucket(id: BucketId,
                       kmers: Array[Array[Int]],
                       taxa: Array[Int]) {

  def merge(other: TaxonBucket, ancestors: ParentMap): TaxonBucket = {
    ???
  }
}