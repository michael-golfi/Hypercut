package dbpart.bucket
import dbpart._

object CoverageBucket {

}

trait CoverageBucket extends Serializable {
  def coverages: Array[Array[Coverage]]

  def kmerCoverages: Iterator[Coverage] = coverages.iterator.flatten

  def sequenceCoverages: Array[Array[Coverage]] = coverages

  def average(xs: Iterable[Double]): Double = xs.sum/xs.size

  def sequenceAvgCoverages: Iterable[Double] =
    sequenceCoverages.map(sc => average(sc.map(_.toDouble)))

  def hasMinCoverage(min: Coverage): Boolean = kmerCoverages.exists(_ >= min)
}

case class ShortCoverageBucket(coverageData: Array[Array[Short]]) extends CoverageBucket {
  def coverages: Array[Array[Coverage]] = coverageData
}