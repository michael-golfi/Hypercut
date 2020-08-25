package hypercut.taxonomic

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaxonSummary {

  def fromClassifiedKmers(kmerClassifications: Iterator[Taxon], order: Int): TaxonSummary = {
    var count = 1
    var last = kmerClassifications.next
    var taxBuffer = new ArrayBuffer[Taxon](200)
    var countBuffer = new ArrayBuffer[Taxon](200)
    for (tax <- kmerClassifications) {
      if (tax == last) {
        count += 1
      } else {
        taxBuffer += last
        countBuffer += count
        last = tax
        count = 1
      }
    }
    taxBuffer += last
    countBuffer += count
    TaxonSummary(order, taxBuffer, countBuffer)
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

  def taxonRepr(t: Taxon) =
    if (t == AMBIGUOUS) "A" else s"$t"

  def stringFromPairs(pairs: Iterator[(Taxon, Int)]): String = {
    val sb = new StringBuilder
    for ((t, c) <- pairs) {
      sb.append(taxonRepr(t))
      sb.append(":")
      sb.append(c)
      if (pairs.hasNext) {
        sb.append(" ")
      }
    }
    sb.toString()
  }
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

  override def toString: String =
    TaxonSummary.stringFromPairs((taxa.iterator zip counts.iterator))

  def asPairs: Iterator[(Taxon, Int)] = (taxa.iterator zip counts.iterator)
}
