package dbpart

import scala.collection.mutable.HashMap
import dbpart.ubucket.EdgeDB
import scala.collection.mutable.{HashSet => MSet}

/**
 * Tracks discovered edges in memory.
 */
final class EdgeSet {
  var data: HashMap[String,MSet[String]] = new HashMap[String, MSet[String]]

  def add(edges: TraversableOnce[(String, String)]) {
    synchronized {
      for ((e, f) <- edges) {
        data.get(e: String) match {
          case Some(old) => old += f
          case None =>
          data += (e -> MSet[String](f))
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
  def writeTo(db: EdgeDB) {
    db.addBulk(data)
  }
}