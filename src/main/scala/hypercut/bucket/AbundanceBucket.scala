package hypercut.bucket
import hypercut._

import scala.collection.mutable.IndexedSeq

object AbundanceBucket {
  //The maximum abundance value that we track. Currently this is an ad hoc limit.
  val abundanceCutoff = 5000.toShort


  def clipAbundance(abund: Long): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund.toShort

  def clipAbundance(abund: Int): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund.toShort

  def clipAbundance(abund: Short): Abundance = if (abund > abundanceCutoff) abundanceCutoff else abund

  def incrementAbundance(abundSeq: IndexedSeq[Abundance], pos: Int, amt: Abundance) = {
    abundSeq(pos) = clipAbundance(abundSeq(pos) + amt)
  }
}

trait AbundanceBucket extends Serializable {
  def abundances: Array[Array[Abundance]]

  def kmerAbundances: Iterator[Abundance] = abundances.iterator.flatten

  def sequenceAbundances: Array[Array[Abundance]] = abundances

  def average(xs: Iterable[Double]): Double = xs.sum/xs.size

  def sequenceAvgAbundances: Iterable[Double] =
    sequenceAbundances.map(sc => average(sc.map(_.toDouble)))

  def hasMinAbundance(min: Abundance): Boolean = kmerAbundances.exists(_ >= min)
}

case class ShortAbundanceBucket(abundanceData: Array[Array[Short]]) extends AbundanceBucket {
  def abundances: Array[Array[Abundance]] = abundanceData
}