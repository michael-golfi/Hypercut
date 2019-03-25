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

/**
 * @param boundary Whether this node is part of the partition boundary (outer edge)
 * and potentially has unknown edges into other partitions.
 * See MacroNode.boundary.
 */
final class KmerNode(val seq: NTSeq, val coverage: Double) extends HasID {
  var seen: Boolean = false
  var noise: Boolean = false
  var boundary: Boolean = false

  override def toString: String = s"$seq($coverage)"

  def k: Int = seq.length()

  def begin: String = seq.substring(0, k - 1)
  def end: String = seq.substring(1, k)
}