package hypercut.taxonomic

import miniasm.genome.bpbuffer.BPBuffer

import scala.collection.mutable
import scala.util.Sorting

object ParentMap {
  val NONE = 0
}

/**
 * Maps each taxon to its parent.
 * @param parents
 */
final case class ParentMap(parents: Array[Taxon]) {
  import ParentMap._

  /**
   * Lowest common ancestor of two taxa.
   * Algorithm from Kraken's krakenutil.cpp.
   * @param tax1
   * @param tax2
   * @return
   */
  def lca(tax1: Taxon, tax2: Taxon): Taxon = {
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
  def resolveTree(hitCounts: collection.Map[Taxon, Int]): Taxon = {
    val maxTaxa = mutable.Set.empty[Taxon]
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

  def classifySequence(taxa: Iterable[Taxon], k: Int): (Taxon, String, Int) = {
    val countedTaxa = taxa.groupBy(x => x).map(x => (x._1, x._2.size))
    val mappingSummaries = countedTaxa.toSeq.map(x => s"${x._1}:${x._2}").mkString(" ")
    //Assuming each counted taxon corresponds to a k-mer from a sequence
    val seqLength = countedTaxa.values.sum + (k-1)
    (resolveTree(countedTaxa), mappingSummaries, seqLength)
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
  def taxonTaggedFromSequences(segmentsTaxa: Iterable[(BPBuffer, Taxon)], k: Taxon): Iterator[(Array[Int], Taxon)] = {
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
  def allLca[T](taxa: Iterable[Taxon]): Taxon = {
    if (taxa.size == 1)
      taxa.head
    else
      taxa.reduce(lca)
  }
}