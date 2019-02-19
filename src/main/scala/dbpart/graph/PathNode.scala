package dbpart.graph
import dbpart._
import dbpart.ubucket.SeqBucketDB

final class PathNode(val seq: NTSeq, val avgCoverage: Double) {
  def numKmers(k: Int): Int = seq.length() - (k-1)
  def kmers(k: Int): Iterator[String] =
    Read.kmers(seq, k)

  var seen: Boolean = false
  var noise: Boolean = false

  override def toString: String = s"$seq($avgCoverage)"
}