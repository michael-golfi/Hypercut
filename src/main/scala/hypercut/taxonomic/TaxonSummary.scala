package hypercut.taxonomic

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaxonSummary {

  //TODO try to come up with a version that doesn't build so many lists
  def split[T](list: List[T]) : List[(T, Int)] = list match {
    case Nil => Nil
    case h::t =>
      val segment = list takeWhile { h == _ }
      val l = segment.length
      (h, l) :: split(list drop l)
  }

  def fromClassifiedKmers(kmerClassifications: List[Taxon], order: Int): TaxonSummary = {
    val (taxa, counts) = split[Taxon](kmerClassifications).unzip
    TaxonSummary(order, taxa.toArray, counts.toArray)
  }

  //TODO find a way to do this without using a map
  def mergeHitCounts(summaries: Iterable[TaxonSummary]): mutable.Map[Taxon, Int] = {
    val m = mutable.Map.empty[Taxon, Int]
    for {
      s <- summaries
      (t, c) <- s.taxa zip s.counts
    } {
      m.get(t) match {
        case Some(oc) => m(t) = (oc + c)
        case _ => m(t) = c
      }
    }
    m
  }

  def concatenate(summaries: Iterable[TaxonSummary]): TaxonSummary = {
    val maxSize = summaries.map(_.counts.size)
    val taxonRet = new ArrayBuffer[Taxon]()
    taxonRet.sizeHint(maxSize)
    val countRet = new ArrayBuffer[Int]()
    countRet.sizeHint(maxSize)

    for (s <- summaries) {
      if (taxonRet.size > 0 && taxonRet(taxonRet.size - 1) == s.taxa(0)) {
        countRet(countRet.size - 1) += s.counts(0)
      } else {
        taxonRet += s.taxa(0)
        countRet += s.counts(0)
      }
      for (i <- 1 until s.taxa.length) {
        taxonRet += s.taxa(i)
        countRet += s.counts(i)
      }
    }
    new TaxonSummary(summaries.head.order, taxonRet, countRet)
  }

  def ambiguous(order: Int, kmers: Int) =
    TaxonSummary(order, Array(AMBIGUOUS), Array(kmers))
}

/**
 * Information about classified k-mers for a consecutive DNA segment.
 *
 * @param order The position of this summary in the order of summaries for a sequence
 *              (not same as absolute position in the sequence)
 * @param taxa Taxa for each classified region (may be repeated)
 * @param counts Counts for each taxon
 *
 */
final case class TaxonSummary(order: Int, taxa: mutable.Seq[Taxon], counts: mutable.Seq[Int]) {
  def taxonRepr(t: Taxon) =
    if (t == AMBIGUOUS) "A" else s"$t"

  override def toString: String = {
    (taxa zip counts).map(hit => s"${taxonRepr(hit._1)}:${hit._2}").mkString(" ")
  }
}
