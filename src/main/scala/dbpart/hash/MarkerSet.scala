package dbpart.hash

import java.nio.ByteBuffer
import java.util.Arrays

import scala.annotation.tailrec
import scala.collection.Seq

object Marker {
  val PackedMarker = "([ACTGUN]+)(\\d+)"r

  def unpack(space: MarkerSpace, key: String) = {
     key match {
       case PackedMarker(tag, pos) => space.get(tag, pos.toInt)
     }
  }
}

/**
 * The attributes of a marker, except its position.
 */
final case class Features(val tag: String, val tagRank: Int, val sortValue: Int = 0) {

  def tagIndex(space: MarkerSpace) = space.byIndex(tag)

  def strongEquivalent(other: Features) = {
    tag == other.tag
  }

  def withSortValue(space: MarkerSpace, value: Int) = space.getFeatures(tag, value)
}

final case class Marker(pos: Int, features: Features) {
  def tag = features.tag
  def sortValue = features.sortValue

  //Note: implicit assumption that pos < 100 when we use this
  lazy val packedString = {
    val r = new StringBuilder
    packInto(r)
    r.toString()
  }

  def packInto(r: StringBuilder) {
    r.append(tag)
    if (pos < 10) {
      r.append("0")
    }
    r.append(pos)
  }

  override def toString = "[%s,%02d]".format(tag, pos)

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

  /**
   * Obtain the canonical version of this marker, to save memory
   */
  def canonical(ms: MarkerSpace): Marker = {
    ms.get(tag, pos, sortValue)
  }

  lazy val rankSort = features.tagRank * 1000 + pos

  //NB this limits the maximum k-mer size (1000)
  /**
   * A metric for sorting by rank.
   * sorts earlier is higher if rank/priority number is lower.
   * @param posInSet position in a set of markers (not in the underlying sequence)
   */
  def withSortValue(space: MarkerSpace, posInSet: Int) =
    copy(features = features.withSortValue(space, 1000 * features.tagRank + posInSet))

  /**
   * Whether the two markers overlap when absolute positions are used.
   */
  def overlaps(other: Marker) = {
    //this.pos should be < other.pos
    (this.pos + this.features.tag.length() > other.pos)
  }
}

object MarkerSet {
  def unpack(space: MarkerSpace, key: String) = {
    if (key == "") {
      //Temporary solution while we think about how to handle the no-markers case
      Console.err.println("Warning: constructed MarkerSet with no markers")
      new MarkerSet(space, List())
    } else {
      new MarkerSet(space, key.split("\\.").map(Marker.unpack(space, _)).toList)
    }
  }

  /**
   * Size of a compact marker set in bytes
   */
  def compactSize(space: MarkerSpace) = 2 * space.n

  def unpackToCompact(space: MarkerSpace, key: String) =
    unpack(space, key).compact

  def uncompactToString(data: Array[Byte], space: MarkerSpace): String = {
    val b = ByteBuffer.wrap(data)
    val r = new StringBuilder
    val n = b.capacity() / 2
    var i = 0
    while (i < n) {
      r.append(space.byPriority(b.get))
      val pos = b.get
      if (pos < 10) {
        r.append("0")
      }
      r.append(pos)
      i += 1
      if (i < n) {
        r.append(".")
      }
    }
    r.toString()
  }

  /**
   * Mainly for testing
   */
  def apply(space: MarkerSpace, tags: Seq[String], positions: Seq[Int]) = {
    val ms = (tags zip positions).map(x => space.get(x._1, x._2))
    new MarkerSet(space, ms.toList)
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

  final def dropHeadMarker(s: List[Marker]) =
    if (s.size > 1) {
      setFirstToZero(s.drop(1))
    } else {
      Seq()
    }

  final def setFirstToZero(s: List[Marker]) = {
    if (!s.isEmpty) {
      s.head.copy(pos = 0) :: s.tail
    } else {
      List()
    }
  }

  @tailrec
  def relativePositionsSorted(space: MarkerSpace, ms: List[Marker],
                        acc: List[Marker]): List[Marker] = {
    ms match {
      case m :: n :: ns => relativePositionsSorted(space, n :: ns,
        n.copy(pos = n.pos - m.pos) :: acc)
      case _ => acc.reverse
    }
  }

  def relativePositionsSorted(space: MarkerSpace, ms: List[Marker]): List[Marker] = {
      ms match {
      case Nil => ms
      case _ => ms.head :: relativePositionsSorted(space, ms, Nil)
    }
  }

  /**
   * Sort markers by position (should be absolute) and change to relative positions
   * (marker intervals)
   */
  def relativePositions(space: MarkerSpace, ms: List[Marker]): List[Marker] = {
    ms match {
      case Nil => ms
      case _ =>
        val bp = ms.sortBy(_.pos)
        bp.head :: relativePositionsSorted(space, bp, Nil)
    }
  }

  def shiftLeft(ms: Seq[Marker], n: Int): Seq[Marker] =
    ms.map(m => m.copy(pos = m.pos - n))

}

/**
 * Compact version of a MarkerSet with fast hashCode and equals.
 */
final case class CompactNode(val data: Array[Byte], override val hashCode: Int) {

  override def equals(other: Any): Boolean = other match {
    case cn: CompactNode =>
      Arrays.equals(data, cn.data)
    case _ => super.equals(other)
  }
}

/*
 * Markers, with relative positions, sorted by absolute position
 */
final case class MarkerSet(space: MarkerSpace, val relativeMarkers: List[Marker]) {
  import MarkerSet._

  var inPartition: Boolean = false

  lazy val packedString = {
    val r = new StringBuilder
    val it = relativeMarkers.iterator
    while (it.hasNext) {
      it.next.packInto(r)
      if (it.hasNext) {
        r.append(".")
      }
    }
    r.toString
  }

  lazy val compact = {
    val r = ByteBuffer.allocate(compactSize(space))
    val it = relativeMarkers.iterator
    var hash = 0
    while (it.hasNext) {
      val m = it.next
      val tag = m.features.tagIndex(space).toByte
      r.put(tag)
      //TODO check/warn about max size of positions, if we're not using short for the position
      val pos = m.pos.toByte
      r.put(pos)
      hash = (hash * 41 + tag) * 41 + pos
    }
    new CompactNode(r.array(), hash)
  }

  override def toString = "ms{" + relativeMarkers.mkString(",") + "}"

  def apply(pos: Int): Marker = relativeMarkers(pos)

  def tagAt(pos: Int): Option[String] = {
    if (relativeMarkers.size > pos) {
      Some(relativeMarkers(pos).tag)
    } else {
      None
    }
  }

  def fromZeroAsArray = new MarkerSet(space, setFirstToZero(relativeMarkers))

  def fromZero = new MarkerSet(space, setFirstToZero(relativeMarkers))


  def relativeByRank =
    relativeMarkers.zipWithIndex.sortBy(m => (space.priorityOf(m._1.tag), m._2)).map(_._1)

  /**
   * Produces a copy where the sort values have been assigned to all markers.
   * Also converts the marker set to a list.
   */
  def fixMarkers = {
    if (relativeByRank.isEmpty) {
      this
    } else {
      val withSortValues = relativeMarkers.zipWithIndex.map(x => x._1.withSortValue(space, x._2)).toList
      new MarkerSet(space, withSortValues)
    }
  }

  def removeMarker(pos: Int,  ms: List[Marker]): List[Marker] = {
    if (pos == 0) {
      ms match {
        case a :: b :: xs =>
          b.copy(pos = a.pos + b.pos) :: xs
        case a :: xs =>
          xs
        case _ => ???
      }
    } else {
      ms match {
        case a :: xs =>
          a :: removeMarker(pos - 1, xs)
        case _ => ???
      }
    }
  }

  lazy val collapse = {
    if (!relativeMarkers.isEmpty) {
      val lowest = relativeMarkers.sortBy(_.rankSort).last
      val i = relativeMarkers.indexOf(lowest)
      new MarkerSet(space, removeMarker(i, relativeMarkers))
    } else {
      new MarkerSet(space, List())
    }
  }

  /**
   * Produce a copy with canonical markers, to save memory.
   */
  def canonical = new MarkerSet(space, relativeMarkers.map(_.canonical(space)))
}