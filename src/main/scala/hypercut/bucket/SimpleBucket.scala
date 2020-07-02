package hypercut.bucket


import hypercut.hash.BucketId
import hypercut.spark.Counting
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
   * Algorithm from Kraken's krakenutil.cpp.
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
    return 1 //root of tree
  }

  /**
   * Take all hit taxa plus ancestors, then return the
   * leaf of the highest weighted leaf-to-root path.
   * Algorithm from Kraken's krakenutil.cpp.
   * @param hitCounts
   */
  def resolveTree(hitCounts: Map[Int, Int]): Int = {
    val maxTaxa = mutable.Set.empty[Int]
    var maxTaxon = 0
    var maxScore = 0
    val it = hitCounts.iterator

    while(it.hasNext) {
      val taxon = it.next._1
      var node = taxon
      var score = 0
      while (node != NONE) {
        score += hitCounts.getOrElse(node, 0)
        node = parents(node)
      }

      if (score > maxScore) {
        maxTaxa.clear()
        maxTaxon = taxon
        maxScore = score
      } else if (score == maxScore) {
        if (maxTaxa.isEmpty)
          maxTaxa += maxTaxon
        maxTaxa += taxon
      }
    }

    if (!maxTaxa.isEmpty) {
      maxTaxa.iterator.reduce(lca)
    } else {
      maxTaxon
    }
  }

  def classifySequence(taxa: Iterable[Int]): Int = {
    val countedTaxa = taxa.groupBy(x => x).map(x => (x._1, x._2.size))
    resolveTree(countedTaxa)
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

  //taxa must be non-empty
  def allLca[T](taxa: Iterable[Int]): Int = {
    if (taxa.size == 1)
      taxa.head
    else
      taxa.reduce(lca)
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

  implicit def ordering[T] = Counting.tagOrdering[T]
  import Counting.KmerOrdering

  /**
   * For tagged sequences that belong to this bucket, classify each one using
   * the LCA algorithm. Return a (tag, taxon) pair for each k-mer that has a hit.
   * @param data
   * @tparam T
   * @return
   */
  def classifyKmers[T](subjects: Iterable[(BPBuffer, T)], k: Int): Iterator[(T, Int)] = {
    val byKmer = subjects.iterator.flatMap(s =>
      s._1.kmersAsArrays(k.toShort).map(km => (km, s._2))
    ).toArray
    Sorting.quickSort(byKmer)

    //Rely on both arrays being sorted
    val bucketIt = kmers.indices.iterator
    val subjectIt = byKmer.iterator
    if (bucketIt.isEmpty || byKmer.isEmpty) {
      return Iterator.empty
    }
    var bi = bucketIt.next
    var subj = subjectIt.next
    while (subjectIt.hasNext && KmerOrdering.compare(subj._1, kmers(bi)) < 0) {
      subjectIt.next
    }

    //The same k-mer may occur multiple times in subjects for different tags (but not in the bucket)
    subjectIt.flatMap(x => {
      while (bucketIt.hasNext && KmerOrdering.compare(x._1, kmers(bi)) > 0) {
        bi = bucketIt.next
      }
      if (KmerOrdering.compare(x._1, kmers(bi)) == 0) {
        Some((x._2, taxa(bi)))
      } else {
        None
      }
    })
  }

  def merge(other: TaxonBucket, ancestors: ParentMap): TaxonBucket = {
    ???
  }
}