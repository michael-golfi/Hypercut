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

  @volatile 
  private var lookup = Map[(String, Boolean, Int), Features]()
    
  def getFeatures(pattern: String, lowestRank: Boolean, sortValue: Int): Features = {
    val key = (pattern, lowestRank, sortValue)
    if (!lookup.contains(key)) {
      synchronized {
        val f = new Features(pattern, lowestRank, sortValue)
        lookup += key -> f
      }
    }
    lookup(key)
  }
  
  def get(pattern: String, pos: Int, lowestRank: Boolean = false,
          sortValue: Int = 0): Marker = {    
    Marker(pos, getFeatures(pattern, lowestRank, sortValue))
  }
    
  def priorityOf(mk: String) = byPriority.indexOf(mk)

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
  
  @tailrec
  private def getMarkers(s: String, remMarkers: List[String], max: Int, soFar: ListBuffer[Marker])  {
    if (!remMarkers.isEmpty && soFar.size < max) {
      allIndexOf(s, remMarkers.head, 0, soFar)
      getMarkers(s, remMarkers.tail, max, soFar)
    }
  }

  //Sort by position, inserting m2 into m1.
  private def mergeMarkers(m1: List[Marker], m2: List[Marker],
                           nextMarkerAt: Int = 0): List[Marker] = {
    if (m1.isEmpty) {
      m2      
    } else if (m2.isEmpty) {
      m1
    } else {
      if (m1.head.pos < m2.head.pos && m1.head.pos >= nextMarkerAt) {              
        m1.head :: mergeMarkers(m1.tail, m2, m1.head.pos + m1.head.tag.length())        
      } else if (m2.head.pos <= m1.head.pos && m2.head.pos >= nextMarkerAt) {
        m2.head :: mergeMarkers(m1, m2.tail, m2.head.pos + m2.head.tag.length())
      } else {
        mergeMarkers(m1.tail, m2.tail, nextMarkerAt)
      }
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
    for (mark <- byPriority) {
      allIndexOf(s, mark, 0, buf)
      r = mergeMarkers(r, buf.toList)
      buf.clear()
    }
    r
  }
  
  /**
   * Top markers sorted by position, relative
   */
  def topMarkers(s: String, max: Int): Seq[Marker] = {
    val buf = ListBuffer[Marker]()
    buf.sizeHint(max)
    getMarkers(s, byPriority.toList, max, buf)
    MarkerSet.relativePositions(this, buf take max)
  }
}