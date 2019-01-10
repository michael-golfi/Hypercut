package dbpart

import scala.collection.Seq

object Marker {
  val PackedMarker = "([ACTGUN]+)(\\d+)"r
  
  def unpack(space: MarkerSpace, key: String) = {
     key match {
       case PackedMarker(tag, pos) => space.get(tag, pos.toInt)
     }
  }
}

final class Features(val tag: String, val lowestRank: Boolean = false,
  val sortValue: Int = 0) {
  
  def strongEquivalent(other: Features) = {
    tag == other.tag && lowestRank == other.lowestRank
  }
  
  def asLowestRank(space: MarkerSpace) = space.getFeatures(tag, true, sortValue)
  
  def withSortValue(space: MarkerSpace, value: Int) = space.getFeatures(tag, lowestRank, value)
}

final case class Marker(pos: Int, features: Features) { 
  def tag = features.tag
  def lowestRank = features.lowestRank
  def sortValue = features.sortValue
  
  lazy val packedString = "%s%02d".format(tag, pos)
  
  override def toString = "[%s,%02d%s]".format(tag, pos, lrs)

  def lrs = if (lowestRank) ":l" else ""
  
  def equivalent(other: Marker) = {
    tag == other.tag && pos == other.pos
  }
  
  def strongEquivalent(other: Marker) = {
    pos == other.pos && ((features eq other.features) || features.strongEquivalent(other.features))
  }  
  
  override def equals(other: Any) = {
    other match {
      case m: Marker => this.strongEquivalent(m)
      case _ => false
    }
  }
  
  override lazy val hashCode: Int = pos.hashCode * 41 + features.hashCode
  
  def asLowestRank(space: MarkerSpace) = if (lowestRank) this else 
    copy(features = features.asLowestRank(space))
  
  /**
   * Obtain the canonical version of this marker, to save memory
   */
  def canonical(ms: MarkerSpace): Marker = {
    ms.get(tag, pos, lowestRank, sortValue)
  }
  
  //NB this limits the maximum k-mer size (1000)
  /**
   * A metric for sorting by rank.
   * sortValue is higher if rank is lower.
   * @param posInSet position in a set of markers (not in the underlying sequence)
   */
  def withSortValue(space: MarkerSpace, posInSet: Int) =
    copy(features = features.withSortValue(space, 1000 * space.priorityOf(tag) + posInSet))
}

object MarkerSet { 
  def unpack(space: MarkerSpace, key: String) = 
    new MarkerSet(space, key.split("\\.").map(Marker.unpack(space, _)).toVector)
  
  /**
   * Mainly for testing
   */
  def apply(space: MarkerSpace, tags: Seq[String], positions: Seq[Int]) = {
    val ms = (tags zip positions).map(x => space.get(x._1, x._2))
    new MarkerSet(space, ms)
  }
      
  final def addToFirst(s: Seq[Marker], n: Int) = {
    if (s.isEmpty) {
      List()
    } else {
      s.head.copy(pos = s.head.pos + n) +: s.tail
    }
  }
  
  /**
   * In a sequence of relative markers, remove the first and update
   * the offsets of the following one.
   */
  final def removeHeadMarker(s: Seq[Marker]) = {
    if (!s.isEmpty) {
      addToFirst(s.tail, s.head.pos)      
    } else {
      Seq()
    }
  }
  
  final def dropHeadMarker(s: Seq[Marker]) =
    if (s.size > 1) {
      setFirstToZero(s.drop(1))      
    } else {
      Seq()
    }
  
  final def setFirstToZero(s: Seq[Marker]) = {
    if (!s.isEmpty) {
      s.head.copy(pos = 0) +: s.tail
    } else {
      Seq()
    }
  }
  
  /**
   * Sort markers by position (should be absolute) and change to relative positions
   * (marker intervals) 
   */
  def relativePositions(space: MarkerSpace, ms: Seq[Marker]): Seq[Marker] = {
    val bp = ms.sortBy(_.pos)
    if (bp.size == 0) {
      return bp
    }
    val first = bp.head
    if (bp.size == 1) {
      bp
    } else {
      val n = bp.size
      val r = Array.fill[Marker](n)(null)
      r(0) = first
      var i = 1
      while(i < n) {
        r(i) = space.get(bp(i).tag, bp(i).pos - bp(i-1).pos)
        i += 1
      }
      r
    }
  }
  
  def shiftLeft(ms: Seq[Marker], n: Int): Seq[Marker] = 
    ms.map(m => m.copy(pos = m.pos - n))
  
}

/*
 * Markers, with relative positions, sorted by absolute position
 */
class MarkerSet(space: MarkerSpace, val relativeMarkers: Seq[Marker]) {
  import MarkerSet._
  
  var tag1: Boolean = false
  
  lazy val packedString = relativeMarkers.map(_.packedString).mkString(".")
  
  override def toString = "ms{" + relativeMarkers.mkString(",") + "}"
  
  override lazy val hashCode: Int = relativeMarkers.hashCode
  
//  override def equals(other: Any) = {
//    other match {
//      case ms: MarkerSet =>
//        sameObjects(relativeMarkers, ms.relativeMarkers)        
//      case _ => false
//    }
//  }
  
  def apply(pos: Int): Marker = relativeMarkers(pos)
  
  final def tagAt(pos: Int): Option[String] = { 
    if (relativeMarkers.size > pos) {
      Some(relativeMarkers(pos).tag)
    } else {
      None
    }
  }
  
  def fromZeroAsArray = new MarkerSet(space, Array() ++ setFirstToZero(relativeMarkers))
  
  def fromZero = new MarkerSet(space, setFirstToZero(relativeMarkers))
  
  /**
   * Whether a sequence of relative markers is allowed to precede another one.
   * @param n The maximum number of markers in a full set. Tracks the maximum number
   *   allowed on the right hand side.
   *   
   * lowestRank on markers must have been set in advance, and may only be set on one marker
   * in each MarkerSet.
   */
  private def canPrecede(s1: Seq[Marker], s2: Seq[Marker], 
                         n: Int,
                         mayRemoveLeftRank: Boolean = true,
                         mayInsertRight: Boolean = true,
                         removedLeftPos: Boolean = false): Boolean = {
    //TODO check size handling - initial marker sets might be smaller than n, 
    //since not enough markers available
    
//    println(s"Now at: $s1 $s2 $mayRemoveLeftRank $mayInsertRight $removedLeftPos")    
//    println(s"SortValues ${s1.headOption map {_.sortValue}} ${s2.headOption map {_.sortValue }}")
//    
    if (s1.isEmpty) {
//      println("Reached end")
      s2.size <= n
    } else if (!s2.isEmpty && (s1.head equivalent s2.head) &&
        canPrecede(s1.tail, s2.tail, n - 1, mayRemoveLeftRank, mayInsertRight, removedLeftPos)) {
//        println("Normal success")
        true
    } else if ((s1.head.lowestRank) && mayRemoveLeftRank && 
        canPrecede(removeHeadMarker(s1), s2, n, false, mayInsertRight, removedLeftPos)) {
//        println("Left rank")
      //the lowest ranked item may disappear once on the left
      //TODO could validate that a corresponding marker with a suitable rank appears on the right
      true
    } else if (!s2.isEmpty && mayInsertRight &&
        (s1.size >= n - 1) &&
        removedLeftPos &&
        s2.head.sortValue > s1.head.sortValue && 
        canPrecede(setFirstToZero(s1), dropHeadMarker(s2), n - 1, mayRemoveLeftRank, false, removedLeftPos)) {
//        println("Right rank")
       //a new item may be inserted once on the right by rank. 
       //Its rank must be lower than the corresponding item on the left.
       //A corresponding drop must have occurred (or s1 must be too small to begin with)
        true
    } else {
//      println(s"fail at: leftRel $s1 rightRel $s2")
      false
    }    
  }
  
  /**
   * Whether this marker set is allowed to precede another one.
   * 
   */
  final def precedes(other: MarkerSet, n: Int): Boolean = {    
//    println(s"Test: $thisRel $otherRel")
    
    //Normal case
    canPrecede(relativeMarkers, other.relativeMarkers, n) ||    
    //Case 2 and 4
      (relativeMarkers.head.pos == 0 && 
          canPrecede(dropHeadMarker(relativeMarkers), setFirstToZero(other.relativeMarkers), 
            n, false, true, true))
  }
  
  def relativeByRank = 
    relativeMarkers.zipWithIndex.sortBy(m => (space.priorityOf(m._1.tag), m._2)).map(_._1) 
   
  /**
   * Produces a copy where both the lowest rank and the ranks of all the markers are set.
   * Also converts all marker sets to lists.
   * Assumes that the lowestRank flag is not set on any markers prior to invocation.
   */
  def fixMarkers = {
    if (relativeByRank.isEmpty) {
      this
    } else {
      val withSortValues = relativeMarkers.zipWithIndex.map(x => x._1.withSortValue(space, x._2)).toList
      val lowest = withSortValues.sortBy(_.sortValue).last
      val i = relativeMarkers.lastIndexOf(lowest)

      new MarkerSet(space, withSortValues.updated(i, lowest.asLowestRank(space)))
    }
  }
  
  /**
   * Produces a copy where lowestRank values have all been set to false.
   */
  def unfixMarkers =     
      new MarkerSet(space, relativeMarkers.map(m =>
        space.get(m.tag, m.pos, false, m.sortValue)))

  /**
   * Produce a copy with canonical markers, to save memory.      
   */
  def canonical = new MarkerSet(space, relativeMarkers.map(_.canonical(space)))
} 