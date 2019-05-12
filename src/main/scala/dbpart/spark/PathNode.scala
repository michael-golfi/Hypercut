package dbpart.spark
import org.apache.spark.graphx._
import dbpart._

/**
 * A node that initially represents a single sequence.
 * Certain nodes become "stop points" where we collect joined sequences.
 */
final case class PathNode(seq: NTSeq, avgCoverage: Double,
  branchInOrLoop: Boolean = false, outDeg: Int = 0,
  collectedSeq: List[String] = Nil,
  minId: VertexId = Long.MaxValue) {

  def begin(k: Int): String = seq.substring(0, k - 1)
  def end(k: Int): String = seq.substring(seq.length() - (k - 1))
  def withoutEnd(k: Int): String = seq.substring(k - 1)

  //Propagate minimum vertex ID seen in a path of messages.
  def observeVertex(id: VertexId) = {
    if (minId > id) {
      copy(minId = id)
    } else {
      this
    }
  }

  def msg = (seq, avgCoverage, minId)

  def branchOut = outDeg > 1

  def stopPoint = (branchInOrLoop || branchOut || outDeg == 0)

  def collect(seq: String, k: Int) = copy(collectedSeq = collectInto(collectedSeq, k, seq))
  def collect(seq: Iterable[String], k: Int) = {
    copy(collectedSeq = seq.foldLeft(collectedSeq)(collectInto(_, k, _)))
  }

  def collectInto(into: List[String], k: Int, seq: String) = {
    if (into.contains(seq)) {
      into
    } else {
      into.find(s => seq.endsWith(s.substring(0, k - 1))) match {
        case Some(s) => (seq + s.substring(k -1)) :: into.filter(_ != s)
        case None    => seq :: into
      }
    }
  }
}