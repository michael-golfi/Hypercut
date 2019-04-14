package dbpart

import scala.collection.mutable.HashMap
import scala.collection.mutable.{ HashSet => MSet }

import dbpart.bucketdb.EdgeDB
import dbpart.hash._
import scala.collection.mutable.Buffer

/**
 * Tracks discovered edges in memory for periodic write to a database.
 * This class is not thread safe, and users must synchronise appropriately.
 */
final class EdgeSet(db: EdgeDB, writeInterval: Option[Int], space: MarkerSpace) {
  var data: HashMap[CompactNode,MSet[CompactNode]] = new HashMap[CompactNode,MSet[CompactNode]]

  var flushedNodeCount: Long = 0
  var edgeCount: Int = 0
  def seenNodes = data.size + flushedNodeCount

  val writeLock = new Object

  def add(edges: Iterable[List[MarkerSet]]) {
    for (es <- edges) {
      MarkerSetExtractor.visitTransitions(es, (e, f) => addEdge(e.compact, f.compact))
    }
  }

  def addEdge(e: CompactNode, f: CompactNode) {
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
        if ((edgeCount % 100000 == 0) &&
          data.size > int) {
          writeTo(db, space)
        }
      case _ =>
    }
  }

  /**
   * Writes (appends) the edges to the provided EdgeDB.
   */
  def writeTo(db: EdgeDB, space: MarkerSpace) = writeLock.synchronized {
    this.synchronized {
      def uncompact(e: Seq[Byte]) = MarkerSet.uncompactToString(e.toArray, space)

      val groups = data.grouped(1000000)
      data = new HashMap[CompactNode, MSet[CompactNode]]
      for (g <- groups) {
        db.addBulk(g.map(x => x._1.data -> x._2.map(_.data).toSeq))
      }
      flushedNodeCount = db.count
    }
  }
}