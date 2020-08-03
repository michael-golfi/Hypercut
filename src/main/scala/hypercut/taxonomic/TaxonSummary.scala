package hypercut.taxonomic

import scala.collection.mutable

object TaxonSummary {

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
    if (summaries.size == 1) summaries.head
    else {
      summaries.tail.foldLeft(summaries.head)(_ append _)
    }
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

  /**
   * Append another taxon summary adjacent to this one, simplifying representation if possible.
   * @param other
   * @return
   */
  def append(other: TaxonSummary): TaxonSummary = {
    if (taxa.last == other.taxa.head) {
      val newCounts = counts ++ other.counts.tail
      newCounts(counts.length - 1) += other.counts(0)
      TaxonSummary(order, taxa ++ other.taxa.tail, newCounts)
    } else {
      TaxonSummary(order, taxa ++ other.taxa, counts ++ other.counts)
    }
  }
}
