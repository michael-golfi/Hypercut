package dbpart.spark
import dbpart._

final case class PathNode(seq: NTSeq, avgCoverage: Double) {
  def begin(k: Int): String = seq.substring(0, k - 1)
  def end(k: Int): String = seq.substring(seq.length() - (k - 1))
}