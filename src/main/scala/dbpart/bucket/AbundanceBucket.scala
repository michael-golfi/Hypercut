package dbpart.bucket
import dbpart._

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