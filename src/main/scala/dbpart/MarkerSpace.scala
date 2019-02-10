package dbpart

import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import scala.collection.Seq

object MarkerSpace {
  val simple = new MarkerSpace(Seq(
        "AT", "AC",
        "GT", "GC"))

  val default = new MarkerSpace(Seq(
    "ATA", "CCT", "AGG",
    "GT", "AC", "GC", "CC", "GG", "AT"))
}

final class MarkerSpace(byPriority: Seq[String]) {
  val maxMotifLength = byPriority.map(_.length()).max
  val minMotifLength = byPriority.map(_.length()).min

  val byFirstChar = Map() ++ byPriority.groupBy(_.charAt(0))

  @volatile
  private var lookup = Map[(String, Boolean, Int), Features]()

  def getFeatures(pattern: String, lowestRank: Boolean, sortValue: Int): Features = {
    val key = (pattern, lowestRank, sortValue)
    if (!lookup.contains(key)) {
      synchronized {
        val f = new Features(pattern, priorityOf(pattern), lowestRank, sortValue)
        lookup += key -> f
      }
    }
    lookup(key)
  }

  def get(pattern: String, pos: Int, lowestRank: Boolean = false,
          sortValue: Int = 0): Marker = {
    Marker(pos, getFeatures(pattern, lowestRank, sortValue))
  }

  val priorityMap = Map() ++ byPriority.zipWithIndex
  def priorityOf(mk: String) = priorityMap(mk)

  @tailrec
  final def allIndexOf(s: String, ptn: String, from: Int, soFar: ListBuffer[Marker]) {
    if (from < s.length()) {
      val i = s.indexOf(ptn, from)
      if (i != -1) {
        soFar += get(ptn, i)
        allIndexOf(s, ptn, i + 1, soFar)
      }
    }
  }

  //Sort by position, inserting m2 into m1.
  //m1 has priority if there are position collisions.
  //TODO: make this tailrec
  private def mergeMarkers(m1: List[Marker], m2: List[Marker],
                           nextMarkerAt: Int = 0): List[Marker] = {
    m1 match {
      case a :: ms =>
        if (m2.isEmpty) {
          m1
        } else {
          val m2h = m2.head
          if (a.pos <= m2h.pos) {
            //m1 has priority
            a :: mergeMarkers(m1.tail, m2, a.pos + 1)
            // && (m2h.pos + m2h.tag.length) <= a.pos
          } else if (m2h.pos >= nextMarkerAt) {
            //Insert one marker from m2
              m2h :: mergeMarkers(m1, m2.tail, m2h.pos + 1)
            } else {
              //Drop one marker from m2
              mergeMarkers(m1, m2.tail, nextMarkerAt)
            }
        }
      case _ => m2.dropWhile(_.pos < nextMarkerAt)
    }
  }

  /**
   * All markers, by absolute position
   */
  def allMarkers(s: String): List[Marker] = {
    var r = List[Marker]()
    val buf = ListBuffer[Marker]()
    buf.sizeHint(10)

    //TODO efficiency here when many markers present
    //When a certain number of markers have been found in every k-length window, there's no
    //point looking for any more. Iterative/incremental lookup?
    //Another source of potential efficiency increases is markers that overlap, e.g. ATG and AT
    for (mark <- byPriority) {
      allIndexOf(s, mark, 0, buf)
      r = mergeMarkers(r, buf.toList)
      buf.clear()
    }
    r
  }
}