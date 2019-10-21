package dbpart.graph
import dbpart._
import dbpart.shortread.Read

final class PathNode(val seq: NTSeq, val avgAbundance: Double) {
  def numKmers(k: Int): Int = seq.length() - (k-1)
  def kmers(k: Int): Iterator[String] =
    Read.kmers(seq, k)

  var seen: Boolean = false
  var noise: Boolean = false

  override def toString: String = s"$seq($avgAbundance)"
}

/**
 * @param boundary Whether this node is part of the partition boundary (outer edge)
 * and potentially has unknown edges into other partitions.
 * See MacroNode.boundary.
 */
final class KmerNode(val seq: NTSeq, val abundance: Double) extends HasID {
  var seen: Boolean = false
  var noise: Boolean = false

  def boundary: Boolean = (boundaryPartition != None)
  var boundaryPartition: Option[Int] = None
  def bndFlag = if(boundary) "b" else ""

  override def toString: String = s"$seq($abundance)$bndFlag"

  def k: Int = seq.length()

  def begin: String = seq.substring(0, k - 1)
  def end: String = seq.substring(1, k)
}