package dbpart

import scala.collection.mutable.HashMap
import dbpart.ubucket.EdgeDB
import scala.collection.mutable.{HashSet => MSet}

/**
 * Tracks discovered edges in memory.
 */
final class EdgeSet(db: EdgeDB, writeInterval: Option[Int], space: MarkerSpace) {
  var data: HashMap[Seq[Byte],MSet[Seq[Byte]]] = new HashMap[Seq[Byte],MSet[Seq[Byte]]]

  val writeLock = new Object

  def canReceiveData = writeLock.synchronized {
    true
  }

  var flushedNodeCount: Int = 0
  var edgeCount: Int = 0
  def seenNodes = data.size + flushedNodeCount

  def add(edges: TraversableOnce[CompactEdge]) {
    synchronized {
      for ((e, f) <- edges) {
        edgeCount += 1
        data.get(e.toSeq) match {
          case Some(old) => old += f.toSeq
          case None =>
          data += (e.toSeq -> MSet[Seq[Byte]](f.toSeq))
          //Added unit value to workaround compiler bug.
          //See https://github.com/scala/bug/issues/10151
          ()
        }

        writeInterval match {
          case Some(int) =>
            //Avoid frequent size check
            if ((edgeCount % 10000 == 0) &&
              data.size > int) {
              writeTo(db, space)
              data = new HashMap[Seq[Byte], MSet[Seq[Byte]]]
            }
          case _ =>
        }
      }
    }
  }

  /**
   * Writes (appends) the edges to the provided EdgeDB.
   */
  def writeTo(db: EdgeDB, space: MarkerSpace) = writeLock.synchronized {
    def uncompact(e: Seq[Byte]) = MarkerSet.uncompactToString(e.toArray, space)

    flushedNodeCount += data.size
    for (g <- data.grouped(1000000)) {
      db.addBulk(g.map(x => uncompact(x._1) -> x._2.toSeq.map(uncompact)))
    }
  }
}