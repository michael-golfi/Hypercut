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
final case class Features(val tag: String, val tagRank: Int) {

  def equivalent(other: Features) = {
    //tagRank is sufficient to identify tag
    tagRank == other.tagRank
  }
}

final case class Motif(pos: Int, features: Features) {
  def tag = features.tag

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

  def fastEquivalent(other: Motif) = {
    pos == other.pos && ((features eq other.features) || features.equivalent(other.features))
  }

  override def equals(other: Any) = {
    other match {
      case m: Motif => this.fastEquivalent(m)
      case _ => false
    }
  }

  override lazy val hashCode: Int = pos.hashCode * 41 + features.hashCode

  /**
   * Obtain the canonical version of this motif, to save memory
   */
  def canonical(ms: MotifSpace): Motif = {
    ms.get(tag, pos)
  }

  lazy val rankSort = features.tagRank * 1000 + pos

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

  def unpackToCompact(space: MotifSpace, key: String) =
    unpack(space, key).compact

  def uncompactToString(data: Array[Byte], space: MotifSpace): String = {
    val b = ByteBuffer.wrap(data)
    val r = new StringBuilder
    val n = b.capacity() / (space.compactBytesPerMotif + 1)
    var i = 0
    while (i < n) {
      if (space.width > 8) {
        r.append(space.byPriority(b.getInt))
      } else if (space.width > 4) {
        r.append(space.byPriority(b.getShort - Short.MinValue))
      } else {
        r.append(space.byPriority(b.get - Byte.MinValue))
      }

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

  @tailrec
  def relativePositionsSorted(space: MotifSpace, ms: List[Motif],
                              acc: List[Motif]): List[Motif] = {
    ms match {
      case m :: n :: ns => relativePositionsSorted(space, n :: ns,
        n.copy(pos = n.pos - m.pos) :: acc)
      case _ => acc.reverse
    }
  }

  /**
   * Sort motifs by position and change absolute to relative positions,
   * and set the first motif position to 0.
   */
  def relativePositionsSortedFirstZero(space: MotifSpace, ms: List[Motif]): List[Motif] = {
      ms match {
      case Nil => ms
      case _ => ms.head.copy(pos = 0) :: relativePositionsSorted(space, ms, Nil)
    }
  }

  /**
   * Sort motifs by position (should initially be absolute) and change to relative positions
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

  def asBuffer(size: Int) = {
    val r = ByteBuffer.allocate(size)
    val it = relativeMotifs.iterator
    var hash = 0
    while (it.hasNext) {
      val m = it.next
      val tag = m.features.tagRank
      if (space.width > 8) {
        r.putInt(tag)
      } else if (space.width > 4) {
        r.putShort((tag + Short.MinValue).toShort)
      } else {
        r.put((tag + Byte.MinValue).toByte)
      }
      //TODO check/warn about max size of positions, if we're not using short for the position
      val pos = m.pos.toByte
      r.put(pos)
      hash = (hash * 41 + tag) * 41 + pos
    }
    (r, hash)
  }

  lazy val compact: CompactNode = {
    val bufHash = asBuffer(space.compactSize)
    new CompactNode(bufHash._1.array(), bufHash._2)
  }

  lazy val compactLong: Long = {
    assert(space.compactSize <= 8)
    val buf = asBuffer(8)._1
    buf.getLong(0)
  }

  override def toString = "ms{" + relativeMotifs.mkString(",") + "}"
}