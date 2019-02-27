package dbpart

import scala.collection.mutable.HashMap
import dbpart.ubucket.EdgeDB

/**
 * Tracks discovered edges in memory.
 */
final class EdgeSet {
  var data: HashMap[String,List[String]] = new HashMap[String, List[String]]

  def add(edges: TraversableOnce[(String, String)]) {
    synchronized {
      for ((e, f) <- edges) {
        val old = data.getOrElse(e, List())
        data += e -> (f :: old).distinct
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