package dbpart

import scala.annotation.tailrec

/**
 * A node that participates in two mutable doubly linked lists:
 * One sorted by rank, one sorted by position.
 */
sealed trait DLNode {
  var prevPos: DLNode = _
  var nextPos: DLNode = _
  var higherRank: DLNode = _
  var lowerRank: DLNode = _
}

/**
 * End marker for both lists
 */
final case class End() extends DLNode

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
    if (nodes.size > 0) {
      //position order
      r.nextPos = nodes.head
      nodes.head.prevPos = r

      val byRank = nodes.sortBy(_.rankSort)
      r.lowerRank = byRank.head
      byRank.head.higherRank = r

      while (i < nodes.size - 1) {
        nodes(i).nextPos = nodes(i + 1)
        nodes(i + 1).prevPos = nodes(i)
        byRank(i).lowerRank = byRank(i + 1)
        byRank(i + 1).higherRank = byRank(i)
        i += 1
      }
      nodes(i).nextPos = r.end
      r.end.prevPos = nodes(i)
      byRank(i).lowerRank = r.end
      r.end.higherRank = nodes(i)
    }
    r
  }

  def linkPos(front: DLNode, middle: DLNode, end: DLNode) {
    front.nextPos = middle
    middle.prevPos = front
    middle.nextPos = end
    end.prevPos = middle
  }

  def linkRank(above: DLNode, middle: DLNode, below: DLNode) {
    above.lowerRank = middle
    middle.higherRank = above
    middle.lowerRank = below
    below.higherRank = middle
  }

  @tailrec
  def takeByRankRec(from: MarkerNode, n: Int, acc: List[Marker]): List[Marker] = {
    if (n == 0) {
      acc.reverse
    } else {
      from.lowerRank match {
        case End() => (from.m :: acc).reverse
        case m: MarkerNode =>
          takeByRankRec(m, n - 1, from.m :: acc)
      }
    }
  }

  @tailrec
  def dropUntilPositionRec(from: MarkerNode, pos: Int) {
    if (from.pos < pos) {
      from.remove()
      from.nextPos match {
        case End() =>
        case m: MarkerNode =>
          dropUntilPositionRec(m, pos)
      }
    }
  }

  @tailrec
  def rankInsertRec(from: MarkerNode, insert: MarkerNode) {
    if (insert.rankSort < from.rankSort) {
      linkRank(from.higherRank, insert, from.lowerRank)
    } else {
      from.nextPos match {
        case End() =>
          linkRank(from, insert, from.nextPos)
        case m: MarkerNode =>
          rankInsertRec(m, insert)
      }
    }
  }

  //Two markers cannot have the same position
//  @tailrec
//  def positionInsertRec(from: MarkerNode, mn: MarkerNode) {
//    if (mn.pos < from.pos) {
//      linkPos(from.prevPos, mn, from)
//    } else {
//      from.nextPos match {
//        case e: End =>
//          linkPos(from, mn, e)
//          linkRank(from, mn, e)
//        case m: MarkerNode =>
//          positionInsertRec(m, mn)
//      }
//    }
//  }
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

  nextPos = End()
  lowerRank = End()
  nextPos.prevPos = this
  lowerRank.higherRank = this

  def iterator = new Iterator[Marker] {
    var current = nextPos
    def hasNext = (current != End())
    def next = {
      val r = current.asInstanceOf[MarkerNode]
      current = current.nextPos
      r.m
    }

  }

  var end: dbpart.End = nextPos.asInstanceOf[End]

//  def insert(mn: MarkerNode) {
//    nextPos match {
//      case e: End =>
//        linkPos(this, mn, e)
//      case m: MarkerNode =>
//        positionInsertRec(m, mn)
//    }
//  }

  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: MarkerNode) {
    lastPosOption match {
      case Some(last) =>
        linkPos(last, mn, end)
        rankInsertRec(nextPos.asInstanceOf[MarkerNode], mn)
      case _ =>
        linkPos(this, mn, end)
        linkRank(this, mn, end)
    }
  }

  def :+= (m: Marker) {
    this :+= MarkerNode(m.pos, m)
  }


  /**
   * Take a specified number of highest ranking items.
   * Does not alter the list.
   */
  def takeByRank(n: Int): List[Marker] = {
    highestRankOption.map(mn =>
      takeByRankRec(mn, n, Nil)
    ).getOrElse(List())
  }

  /**
   * Removes items before the given position.
   */
  def dropUntilPosition(pos: Int) {
    for (mn <- firstPosOption) {
      dropUntilPositionRec(mn, pos)
    }
  }

  /**
   * Does not alter the list
   */
  def firstPosOption: Option[MarkerNode] = nextPos match {
    case End() => None
    case mn: MarkerNode => Some(mn)
  }

  /**
   * Does not alter the list
   */
  def lastPosOption: Option[MarkerNode] = end.prevPos match {
    case PosRankList() => None
    case mn: MarkerNode => Some(mn)
  }

  /**
   * Does not alter the list
   */
  def highestRankOption: Option[MarkerNode] = lowerRank match {
    case End() => None
    case mn: MarkerNode => Some(mn)
  }

  /**
   * Does not alter the list
   */
  def lowestRankOption: Option[MarkerNode] = end.higherRank match {
    case PosRankList() => None
    case mn: MarkerNode => Some(mn)
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


  //NB this imposes a maximum length on analysed sequences for now
  def rankSort = m.features.tagRank * 1000 + pos

  /**
   * Remove this node from both of the two lists.
   */
  def remove() {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos
    higherRank.lowerRank = lowerRank
    lowerRank.higherRank = higherRank
  }
}