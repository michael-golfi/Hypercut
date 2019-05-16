package dbpart.spark
import org.apache.spark.graphx._
import dbpart._
import miniasm.genome.util.DNAHelpers

/**
 * A node that initially represents a single sequence.
 * Certain nodes become "stop points" where we collect joined sequences.
 */
final case class PathNode(seq: NTSeq, avgCoverage: Double,
  inLoop: Boolean = false, outDeg: Int = 0,
  inDeg: Int = 0,
  seenId: VertexId = Long.MaxValue,
  seqPos: Int = 0) {

  def begin(k: Int): String = DNAHelpers.kmerPrefix(seq, k)
  def end(k: Int): String = DNAHelpers.kmerSuffix(seq, k)
  def withoutEnd(k: Int): String = seq.substring(k - 1)

  def msg = (seenId, seqPos + 1)

  def branchOut = outDeg > 1

  def branchIn = inDeg > 1

  def startPoint = (branchIn || branchOut || inDeg == 0)

  def stopPoint = (branchIn || inLoop || branchOut)

  def inPath = (seqPos > 0)
}