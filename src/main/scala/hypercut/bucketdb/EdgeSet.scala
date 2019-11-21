package hypercut.bucketdb

import scala.collection.mutable.HashMap
import scala.collection.mutable.{ HashSet => MSet }
import hypercut.hash._
import scala.annotation.migration
import scala.annotation.tailrec

/**
 * Tracks discovered edges in memory for periodic write to a database.
 * This class is not thread safe, and users must synchronise appropriately.
 */
final class EdgeSet(db: EdgeDB, writeInterval: Option[Int]) {
  var data: HashMap[CompactNode,MSet[CompactNode]] = new HashMap[CompactNode,MSet[CompactNode]]

  var flushedNodeCount: Long = 0
  var edgeCount: Int = 0
  def seenNodes = synchronized {
    data.size + flushedNodeCount
  }

  val writeLock = new Object

  //Number of nodes plus number of distinct edges stored.
  def fullSize = {
    //Estimate size through an average since the full calculation is costly
    val sample = data.values.take(10000)
    val avgSize = sample.toSeq.map(x => x.size).sum/sample.size
    val nodes = data.size
    val r = nodes + nodes * avgSize
    r
  }

  def add(edges: TraversableOnce[List[MotifSet]]) = synchronized {
    for (es <- edges) {
      MotifSetExtractor.visitTransitions(es, (e, f) => addEdge(e.compact, f.compact))
    }
  }

  private def addEdge(e: CompactNode, f: CompactNode) {
    edgeCount += 1
    data.get(e) match {
      case Some(old) => old += f
      case None =>
        data += (e -> MSet(f))
        //Added unit value to workaround compiler bug.
        //See https://github.com/scala/bug/issues/10151
        ()
    }

    writeInterval match {
      case Some(int) =>
        //Avoid frequent size check
        if ((edgeCount % 1000000 == 0) &&
          fullSize > int) {
          writeTo(db)
        }
      case _ =>
    }
  }

  /**
   * Writes (appends) the edges to the provided EdgeDB.
   */
  def writeTo(db: EdgeDB) = synchronized {
    for (g <- data.grouped(1000000)) {
      db.addBulk(g.map(x => x._1.data -> x._2.map(_.data).toSeq))
    }
    data.clear()
    flushedNodeCount = db.count
  }
}