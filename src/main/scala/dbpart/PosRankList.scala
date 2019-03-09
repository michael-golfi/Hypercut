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
  def takeByRankRec(from: DLNode, n: Int, acc: List[Marker]): List[Marker] = {
    if (n == 0) {
      acc.reverse
    } else {
      from match {
        case End() => acc.reverse
        case m: MarkerNode =>
          acc match {
            case a :: as =>
              if (a.overlaps(m.m)) {
                if (a.rankSort <= m.m.rankSort) {
                  takeByRankRec(m.lowerRank, n, acc)
                } else {
                  takeByRankRec(m.lowerRank, n, m.m :: as)
                }
              } else {
                takeByRankRec(m.lowerRank, n - 1, m.m :: acc)
              }
            case _ => takeByRankRec(m.lowerRank, n - 1, m.m :: acc)
          }
      }
    }
  }

  @tailrec
  def dropUntilPositionRec(from: MarkerNode, pos: Int, space: MarkerSpace) {
    if (from.pos < pos + space.minPermittedStartOffset(from.m.features.tag)) {
      from.remove()
      from.nextPos match {
        case End() =>
        case m: MarkerNode =>
          dropUntilPositionRec(m, pos, space)
      }
    }
  }

  @tailrec
  def rankInsertRec(from: MarkerNode, insert: MarkerNode) {
    if (insert.rankSort < from.rankSort) {
      linkRank(from.higherRank, insert, from)
    } else {
      from.lowerRank match {
        case End() =>
          linkRank(from, insert, from.lowerRank)
        case m: MarkerNode =>
          rankInsertRec(m, insert)
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

  def rankIterator = new Iterator[Marker] {
    var current = lowerRank
    def hasNext = (current != End())
    def next = {
      val r = current.asInstanceOf[MarkerNode]
      current = current.lowerRank
      r.m
    }
  }

  var end: dbpart.End = nextPos.asInstanceOf[End]

  def rankInsert(insert: MarkerNode) {
    lowerRank match {
      case e: End =>
        linkRank(this, insert, e)
      case mn: MarkerNode =>
        rankInsertRec(mn, insert)
    }
  }


  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: MarkerNode) {
    lastPosOption match {
      case Some(last) =>
        linkPos(last, mn, end)
        rankInsert(mn)
      case _ =>
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
    highestRankOption.map(mn =>
      takeByRank(mn, n)
    ).getOrElse(List())
  }

  /**
   * Returns the resolved overlap when a and b are ordered,
   * and the size of the resulting list minus 1.
   */
  private def resolveOverlap(a: Marker, b: Marker): (List[Marker], Int) = {
    if (a.overlaps(b)) {
      if (a.rankSort <= b.rankSort) {
        (List(a), 0)
      } else {
        (List(b), 0)
      }
    } else {
      (List(a, b), 1)
    }
  }

  /**
   * before, x and after are ordered by position.
   * before and after do not overlap.
   * x potentially overlaps with one or both
   */
  private def resolveOverlap(before: Marker, x: Marker, after: Marker): (List[Marker], Int) = {
    var r = List[Marker]()
    if (before.overlaps(x) && x.overlaps(after)) {
      if (x.rankSort < before.rankSort && x.rankSort < after.rankSort) {
        (List(x), -1)
      } else {
        (List(before, after), 0)
      }
    } else if (x.overlaps(after)) {
      val r = resolveOverlap(x, after)
      (before :: r._1, r._2)
    } else {
      val r = resolveOverlap(before, x)
      (r._1 :+ after, r._2)
    }
  }

  /**
   * Insert by position and edit out any overlaps
   * Returns the size delta of the list (0 to 1)
   */
  private def insertPosNoOverlap(into: List[Marker], ins: Marker): (List[Marker], Int) = {
    into match {
      case a :: b :: xs =>
        if (ins.pos > a.pos && ins.pos < b.pos) {
          val r = resolveOverlap(a, ins, b)
          (r._1 ::: xs, r._2)
        } else if (ins.pos < a.pos) {
          //potentially a single overlap to resolve
          val r = if (ins.pos > a.pos) {
            resolveOverlap(a, ins)
          } else {
            resolveOverlap(ins, a)
          }
          (r._1 ::: (b :: xs), r._2)
        } else {
          val r = insertPosNoOverlap(b :: xs, ins)
          (a :: r._1, r._2)
        }
      case a :: Nil =>
        //potentially a single overlap to resolve
        if (ins.pos > a.pos) {
          resolveOverlap(a, ins)
        } else {
          resolveOverlap(ins, a)
        }
      case _ => (List(ins), 1)
    }
  }

  private def takeByRank(from: MarkerNode, n: Int): List[Marker] = {
    var cur = from
    var r = List(cur.m)
    var rem = n - 1
    while (cur.lowerRank != End() && rem > 0) {
      cur = cur.lowerRank.asInstanceOf[MarkerNode]
      val (nr, delta) = insertPosNoOverlap(r, cur.m)
//      println(this)
//      println(nr)
//      for (pair <- nr.sliding(2); if pair.size > 1) {
//        if(pair(0).overlaps(pair(1))) {
//          assert(false)
//        }
//      }
      r = nr
      rem -= delta
    }
    r
  }

  /**
   * Removes items before the given position.
   */
  def dropUntilPosition(pos: Int, space: MarkerSpace) {
    for (mn <- firstPosOption) {
      dropUntilPositionRec(mn, pos, space)
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

  override def toString = {
    "PList(" + this.map(_.packedString).mkString(" ") + ")\n" +
    "  RList(" + this.rankIterator.map(_.packedString).mkString(" ") + ")\n"
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
  lazy val rankSort = m.rankSort

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