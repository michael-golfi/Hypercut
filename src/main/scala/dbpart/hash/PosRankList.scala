package dbpart.hash

import scala.Left
import scala.Right
import scala.util.Either.MergeableEither
import scala.annotation.tailrec

/**
 * A node that participates in two mutable doubly linked lists:
 * One sorted by rank, one sorted by position.
 */
sealed trait DLNode {
  type Backward = Either[PosRankList, MarkerNode]
  type Forward = Either[End, MarkerNode]
  var prevPos: Backward = _
  var nextPos: Forward = _
  var higherRank: Backward = _
  var lowerRank: Forward = _

  def asForwardLink: Forward
  def asBackwardLink: Backward
}

/**
 * End marker for both lists
 */
final case class End() extends DLNode {
  def asForwardLink = Left(this)
  def asBackwardLink = ???
}

object PosRankList {

  /**
   * Build a list from nodes with absolute positions.
   */
  def apply(nodes: Seq[Marker]) = {
    fromNodes(nodes.map(m => MarkerNode(m.pos, m)))
  }

  /**
   * Build a list from position-sorted nodes.
   */
  def fromNodes(nodes: Seq[MarkerNode]) = {
    var i = 0
    var r = new PosRankList()
    var prior: DLNode = r
    if (nodes.nonEmpty) {
      //position order
      r.nextPos = Right(nodes.head)
      nodes.head.prevPos = Left(r)

      val byPos = nodes.toArray
      val byRank = nodes.sortBy(_.rankSort).toArray
      r.lowerRank = Right(byRank.head)
      byRank.head.higherRank = Left(r)

      while (i < nodes.size - 1) {
        byPos(i).nextPos = Right(byPos(i + 1))
        byPos(i + 1).prevPos = Right(byPos(i))
        byRank(i).lowerRank = Right(byRank(i + 1))
        byRank(i + 1).higherRank = Right(byRank(i))
        i += 1
      }
      byPos(i).nextPos = Left(r.end)
      r.end.prevPos = Right(byPos(i))
      byRank(i).lowerRank = Left(r.end)
      r.end.higherRank = Right(byRank(i))
    }
    r
  }

  def linkPos(before: DLNode, middle: MarkerNode, after: DLNode) {
    before.nextPos = Right(middle)
    middle.prevPos = before.asBackwardLink
    middle.nextPos = after.asForwardLink
    after.prevPos = Right(middle)
  }

  def linkRank(above: DLNode, middle: MarkerNode, below: DLNode) {
    above.lowerRank = Right(middle)
    middle.higherRank = above.asBackwardLink
    middle.lowerRank = below.asForwardLink
    below.higherRank = Right(middle)
  }

  @tailrec
  def dropUntilPositionRec(from: MarkerNode, pos: Int, space: MarkerSpace) {
    if (from.pos < pos + space.minPermittedStartOffset(from.m.features.tag)) {
      from.remove()
      from.nextPos match {
        case Left(_) =>
        case Right(m) =>
          dropUntilPositionRec(m, pos, space)
      }
    }
  }

  @tailrec
  def rankInsertRec(from: MarkerNode, insert: MarkerNode) {
    if (insert.rankSort < from.rankSort) {
      linkRank(from.higherRank.merge, insert, from)
    } else {
      from.lowerRank match {
        case Left(e) => linkRank(from, insert, e)
        case Right(m) => rankInsertRec(m, insert)
      }
    }
  }
}

/**
 * The PosRankList maintains two mutable doubly linked lists, corresponding to
 * position and rank ordering of items.
 *
 * This class is the start marker (highest (minimum) rank, lowest pos) for both lists,
 * and also the main public interface.
 */
final case class PosRankList() extends DLNode with Iterable[Marker] {
  import PosRankList._

  nextPos = Left(End())
  lowerRank = Left(End())
  nextPos.left.get.prevPos = Left(this)
  lowerRank.left.get.higherRank = Left(this)

  def asForwardLink: Either[End, MarkerNode] = ???
  def asBackwardLink = Left(this)

  def iterator = new Iterator[Marker] {
    var current = nextPos
    def hasNext = current.isRight
    def next = {
      val r = current.right.get
      current = current.right.flatMap(_.nextPos)
      r.m
    }
  }

  def rankIterator = new Iterator[Marker] {
    var current = lowerRank
    def hasNext = current.isRight
    def next = {
      val r = current.right.get
      current = current.right.flatMap(_.lowerRank)
      r.m
    }
  }

  val end: End = nextPos.left.get

  def rankInsert(insert: MarkerNode) {
    lowerRank match {
      case Left(e)   => linkRank(this, insert, e)
      case Right(mn) => rankInsertRec(mn, insert)
    }
  }

  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: MarkerNode) {
    end.prevPos match {
      case Right(last) =>
        linkPos(last, mn, end)
        rankInsert(mn)
      case Left(_) =>
        linkPos(this, mn, end)
        linkRank(this, mn, end)
    }
  }

  /**
   * Append a marker at the final position, and insert at the correct rank order.
   */
  def :+= (m: Marker) {
    this :+= MarkerNode(m.pos, m)
  }

  /**
   * Take a specified number of highest ranking items.
   * Does not alter the list.
   */
  def takeByRank(n: Int): List[Marker] = {
    lowerRank match {
      case Right(highest) => takeByRank(highest, n)
      case _ => List()
    }
  }

  private def takeByRank(from: MarkerNode, n: Int): List[Marker] = {
    new RankListBuilder(from, n).build
  }

  /**
   * Removes items that can only be parsed
   * before the given sequence position, given the constraints
   * of the given MarkerSpace (min permitted start offset, etc)
   */
  def dropUntilPosition(pos: Int, space: MarkerSpace) {
    nextPos match {
      case Right(mn) => dropUntilPositionRec(mn, pos, space)
      case _ =>
    }
  }

  override def toString = {
    "PList(" + this.map(_.packedString).mkString(" ") + ")\n" +
    "  RList(" + this.rankIterator.map(_.packedString).mkString(" ") + ")\n"
  }
}

final class RankListBuilder(from: MarkerNode, n: Int) {
  //Number of remaining items to generate for the resulting output list
  private[this] var rem: Int = n

  /**
   * Returns the resolved overlap when a and b are ordered.
   */
  def resolveOverlap(a: Marker, b: Marker, xs: List[Marker] = Nil): List[Marker] = {
    if (a.overlaps(b)) {
      if (a.rankSort <= b.rankSort) {
        a :: xs
      } else {
        b :: xs
      }
    } else {
      rem -= 1
      a :: b :: xs
    }
  }

  /**
   * before, x and after are ordered by position.
   * before and after do not overlap.
   * x potentially overlaps with one or both
   */
  def resolveOverlap(before: Marker, x: Marker, after: Marker, xs: List[Marker]): List[Marker] = {
    if (before.overlaps(x) && x.overlaps(after)) {
      if (x.rankSort < before.rankSort && x.rankSort < after.rankSort) {
        //Net removal of one item from the list, not insertion
        rem += 1
        x :: xs
      } else {
        before :: after :: xs
      }
    } else if (x.overlaps(after)) {
      before :: resolveOverlap(x, after, xs)
    } else {
      resolveOverlap(before, x, after :: xs)
    }
  }

  /**
   * Insert by position and edit out any overlaps
   */
  def insertPosNoOverlap(into: List[Marker], ins: Marker): List[Marker] = {
    into match {
      case a :: b :: xs =>
        if (ins.pos > a.pos && ins.pos < b.pos) {
          resolveOverlap(a, ins, b, xs)
        } else if (ins.pos < a.pos) {
          //potentially a single overlap to resolve
          if (ins.pos > a.pos) {
            resolveOverlap(a, ins, b :: xs)
          } else {
            resolveOverlap(ins, a, b :: xs)
          }
        } else {
          a :: insertPosNoOverlap(b :: xs, ins)
        }
      case a :: Nil =>
        //potentially a single overlap to resolve
        if (ins.pos > a.pos) {
          resolveOverlap(a, ins)
        } else {
          resolveOverlap(ins, a)
        }
      case _ =>
        rem -= 1
        ins :: Nil
    }
  }

  def build = {
    var cur = from
    var r = cur.m :: Nil
    rem = n - 1
    while (cur.lowerRank.isRight && rem > 0) {
      cur = cur.lowerRank.right.get
      val nr = insertPosNoOverlap(r, cur.m)
      //      println(this)
      //      println(nr)
      //      for (pair <- nr.sliding(2); if pair.size > 1) {
      //        if(pair(0).overlaps(pair(1))) {
      //          assert(false)
      //        }
      //      }
      r = nr
    }
    r
  }
}

/**
 * A node that participates in two doubly linked lists, sorted by position and by rank,
 * respectively.
 *
 * Position is absolute.
 */
final case class MarkerNode(pos: Int, m: Marker) extends DLNode {
  import PosRankList._

  def asForwardLink = Right(this)
  def asBackwardLink = Right(this)

  //NB this imposes a maximum length on analysed sequences for now
  lazy val rankSort = m.rankSort

  /**
   * Remove this node from both of the two lists.
   */
  def remove() {
    prevPos.merge.nextPos = nextPos
    nextPos.merge.prevPos = prevPos
    higherRank.merge.lowerRank = lowerRank
    lowerRank.merge.higherRank = higherRank
  }
}

/**
 * Avoid recomputing takeByRank(n) by using a smart cache.
 */
final class TopRankCache(list: PosRankList, n: Int) {
  var cache: Option[List[Marker]] = None
  var firstByPos: Marker = _
  var lowestRank: Int = _
  var cacheLength: Int = 0

  def :+= (m: Marker) {
    list :+= m
    if (m.rankSort < lowestRank || cacheLength < n) {
      //Force recompute
      cache = None
    }
  }

  def dropUntilPosition(pos: Int, space: MarkerSpace) {
    list.dropUntilPosition(pos, space)
    if (firstByPos != null &&
        pos + space.minPermittedStartOffset(firstByPos.tag) > firstByPos.pos) {
      //Force recompute
      cache = None
    }
  }

  def takeByRank() = {
    cache match {
      case Some(c) => c
      case _ =>
        val r = list.takeByRank(n)
        if (!r.isEmpty) {
          cache = Some(r)
          cacheLength = r.length
          firstByPos = r.head
          lowestRank = r.map(_.rankSort).max
        }
        r
    }
  }
}