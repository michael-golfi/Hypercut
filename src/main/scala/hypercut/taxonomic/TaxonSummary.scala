package hypercut.taxonomic

import scala.annotation.tailrec
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

  @tailrec
  def mergeHitCounts(rem: List[(Taxon, Int)], acc: List[(Taxon, Int)] = Nil): List[(Taxon, Int)] = {
    rem match {
      case (x, xc) :: (y, yc) :: ys =>
        if (x == y) {
          mergeHitCounts((y, yc + xc) :: ys, acc)
        }  else {
          mergeHitCounts((y, yc) :: ys, (x, xc) :: acc)
        }
      case x :: xs => x :: acc
      case Nil => acc
    }
  }

  //TODO find a way to do this without using a map
  def mergeHitCounts(summaries: Iterable[TaxonSummary]): mutable.Map[Taxon, Int] = {
    val all = summaries.toList.flatMap(_.asPairs).sorted
    mutable.Map.empty ++ mergeHitCounts(all)
  }

  def concatenate(summaries: Iterable[TaxonSummary]): TaxonSummary = {
    val maxSize = summaries.map(_.counts.size).sum
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

  override def toString: String =
    (taxa zip counts).map(hit => s"${taxonRepr(hit._1)}:${hit._2}").mkString(" ")

  def asPairs: Iterator[(Taxon, Int)] = (taxa.iterator zip counts.iterator)
}
