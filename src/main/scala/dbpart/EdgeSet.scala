package dbpart

import scala.collection.mutable.HashMap
import dbpart.ubucket.EdgeDB
import scala.collection.mutable.{HashSet => MSet}

/**
 * Tracks discovered edges in memory.
 */
final class EdgeSet {
  var data: HashMap[Seq[Byte],MSet[Seq[Byte]]] = new HashMap[Seq[Byte],MSet[Seq[Byte]]]

  def add(edges: TraversableOnce[CompactEdge]) {
    synchronized {
      for ((e, f) <- edges) {
        data.get(e.toSeq) match {
          case Some(old) => old += f.toSeq
          case None =>
          data += (e.toSeq -> MSet[Seq[Byte]](f.toSeq))
          //Added unit value to workaround compiler bug.
          //See https://github.com/scala/bug/issues/10151
          ()
        }
      }
    }
  }

  /**
   * Writes (appends) the edges to the provided EdgeDB.
   */
  def writeTo(db: EdgeDB, space: MarkerSpace) {
    def uncompact(e: Seq[Byte]) = MarkerSet.uncompactToString(e.toArray, space)

    for (g <- data.grouped(100000)) {
      db.addBulk(g.map(x => uncompact(x._1) -> x._2.toSeq.map(uncompact)))
    }
  }
}