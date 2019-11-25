package hypercut.hash

import java.nio.ByteBuffer
import java.util.Arrays

import scala.annotation.tailrec
import scala.collection.Seq
import scala.language.postfixOps

object Motif {
  val PackedMotif = "([ACTGUN]+)(\\d+)"r

  def unpack(space: MotifSpace, key: String) = {
     key match {
       case PackedMotif(tag, pos) => space.get(tag, pos.toInt)
     }
  }
}

/**
 * The attributes of a motif, except its position.
 */
final case class Features(val tag: String, val tagRank: Int, val sortValue: Int = 0) {

  def tagIndex(space: MotifSpace) = space.byIndex(tag)

  def strongEquivalent(other: Features) = {
    tag == other.tag
  }

  def withSortValue(space: MotifSpace, value: Int) = space.getFeatures(tag, value)
}

final case class Motif(pos: Int, features: Features) {
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

  def equivalent(other: Motif) = {
    tag == other.tag && pos == other.pos
  }

  def strongEquivalent(other: Motif) = {
    pos == other.pos && ((features eq other.features) || features.strongEquivalent(other.features))
  }

  override def equals(other: Any) = {
    other match {
      case m: Motif => this.strongEquivalent(m)
      case _ => false
    }
  }

  override lazy val hashCode: Int = pos.hashCode * 41 + features.hashCode

  /**
   * Obtain the canonical version of this motif, to save memory
   */
  def canonical(ms: MotifSpace): Motif = {
    ms.get(tag, pos, sortValue)
  }

  lazy val rankSort = features.tagRank * 1000 + pos

  //NB this limits the maximum k-mer size (1000)
  /**
   * A metric for sorting by rank.
   * sorts earlier is higher if rank/priority number is lower.
   * @param posInSet position in a set of motifs (not in the underlying sequence)
   */
  def withSortValue(space: MotifSpace, posInSet: Int) =
    copy(features = features.withSortValue(space, 1000 * features.tagRank + posInSet))

  /**
   * Whether the two motifs overlap when absolute positions are used.
   */
  def overlaps(other: Motif) = {
    //this.pos should be < other.pos
    (this.pos + this.features.tag.length() > other.pos)
  }
}

object MotifSet {
  def unpack(space: MotifSpace, key: String) = {
    if (key == "") {
      //Temporary solution while we think about how to handle the no-motifs case
      Console.err.println("Warning: constructed MotifSet with no motifs")
      new MotifSet(space, List())
    } else {
      new MotifSet(space, key.split("\\.").map(Motif.unpack(space, _)).toList)
    }
  }

  /**
   * Size of a compact motif set in bytes
   */
  def compactSize(space: MotifSpace) = 2 * space.n

  def unpackToCompact(space: MotifSpace, key: String) =
    unpack(space, key).compact

  def uncompactToString(data: Array[Byte], space: MotifSpace): String = {
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
  def apply(space: MotifSpace, tags: Seq[String], positions: Seq[Int]) = {
    val ms = (tags zip positions).map(x => space.get(x._1, x._2))
    new MotifSet(space, ms.toList)
  }

  def addToFirst(s: Seq[Motif], n: Int) = {
    if (s.isEmpty) {
      List()
    } else {
      s.head.copy(pos = s.head.pos + n) +: s.tail
    }
  }

  def setFirstToZero(s: List[Motif]) = {
    if (s.nonEmpty) {
      s.head.copy(pos = 0) :: s.tail
    } else {
      List()
    }
  }

  @tailrec
  def relativePositionsSorted(space: MotifSpace, ms: List[Motif],
                              acc: List[Motif]): List[Motif] = {
    ms match {
      case m :: n :: ns => relativePositionsSorted(space, n :: ns,
        n.copy(pos = n.pos - m.pos) :: acc)
      case _ => acc.reverse
    }
  }

  def relativePositionsSorted(space: MotifSpace, ms: List[Motif]): List[Motif] = {
      ms match {
      case Nil => ms
      case _ => ms.head :: relativePositionsSorted(space, ms, Nil)
    }
  }

  /**
   * Sort motifs by position (should be absolute) and change to relative positions
   * (motif intervals)
   */
  def relativePositions(space: MotifSpace, ms: List[Motif]): List[Motif] = {
    ms match {
      case Nil => ms
      case _ =>
        val bp = ms.sortBy(_.pos)
        bp.head :: relativePositionsSorted(space, bp, Nil)
    }
  }

  def shiftLeft(ms: Seq[Motif], n: Int): Seq[Motif] =
    ms.map(m => m.copy(pos = m.pos - n))

}

/**
 * Compact version of a MotifSet with fast hashCode and equals.
 */
final case class CompactNode(val data: Array[Byte], override val hashCode: Int) {

  override def equals(other: Any): Boolean = other match {
    case cn: CompactNode =>
      Arrays.equals(data, cn.data)
    case _ => super.equals(other)
  }
}

/*
 * Motifss, with relative positions, sorted by absolute position
 */
final case class MotifSet(space: MotifSpace, val relativeMotifs: List[Motif]) {
  import MotifSet._

  var inPartition: Boolean = false

  lazy val packedString = {
    val r = new StringBuilder
    val it = relativeMotifs.iterator
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
    val it = relativeMotifs.iterator
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

  override def toString = "ms{" + relativeMotifs.mkString(",") + "}"

  def fromZeroAsArray = new MotifSet(space, setFirstToZero(relativeMotifs))

  def fromZero = new MotifSet(space, setFirstToZero(relativeMotifs))

  def relativeByRank =
    relativeMotifs.zipWithIndex.sortBy(m => (space.priorityOf(m._1.tag), m._2)).map(_._1)

  /**
   * Produces a copy where the sort values have been assigned to all motifs.
   * Also converts the motifs set to a list.
   */
  def fixMotifs = {
    if (relativeByRank.isEmpty) {
      this
    } else {
      val withSortValues = relativeMotifs.zipWithIndex.map(x => x._1.withSortValue(space, x._2)).toList
      new MotifSet(space, withSortValues)
    }
  }

  def removeMotif(pos: Int, ms: List[Motif]): List[Motif] = {
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
          a :: removeMotif(pos - 1, xs)
        case _ => ???
      }
    }
  }

  lazy val collapse = {
    if (relativeMotifs.nonEmpty) {
      val lowest = relativeMotifs.sortBy(_.rankSort).last
      val i = relativeMotifs.indexOf(lowest)
      new MotifSet(space, removeMotif(i, relativeMotifs))
    } else {
      new MotifSet(space, List())
    }
  }

  /**
   * Produce a copy with canonical motifs, to save memory.
   */
  def canonical = new MotifSet(space, relativeMotifs.map(_.canonical(space)))
}