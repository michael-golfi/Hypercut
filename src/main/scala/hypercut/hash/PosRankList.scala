package hypercut.hash

import hypercut.hash.PosRankList.treeLeftEdge

import scala.Left
import scala.Right
import scala.util.Either.MergeableEither
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * A node that participates in two mutable doubly linked lists:
 * One sorted by rank, one sorted by position.
 */
sealed trait DLNode {
  type Backward = Either[PosRankList, MotifNode]
  type Forward = Either[End, MotifNode]
  var prevPos: Backward = _
  var nextPos: Forward = _

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
  def apply(nodes: Seq[Motif]) = {
    fromNodes(nodes.map(m => MotifNode(m.pos, m)))
  }

  /**
   * Build a list from position-sorted nodes.
   */
  def fromNodes(nodes: Seq[MotifNode]) = {
    var i = 0
    var r = new PosRankList()

    for { n <- nodes } {
      r :+= n
    }
    r
  }

  def linkPos(before: DLNode, middle: MotifNode, after: DLNode) {
    before.nextPos = Right(middle)
    middle.prevPos = before.asBackwardLink
    middle.nextPos = after.asForwardLink
    after.prevPos = Right(middle)
  }

  @tailrec
  def dropUntilPositionRec(from: MotifNode, pos: Int, space: MotifSpace, top: PosRankList) {
    if (from.pos < pos + space.minPermittedStartOffset(from.m.features.tag)) {
      from.remove(top)
      from.nextPos match {
        case Left(_) =>
        case Right(m) =>  dropUntilPositionRec(m, pos, space, top)
      }
    }
  }

  @tailrec
  def treeInsert(root: MotifNode, value: MotifNode) {
    if (value < root) {
      root.rankLeft match {
        case Some(left) =>
          treeInsert(left, value)
        case None =>
          root.rankLeft = Some(value)
      }
    } else {
      root.rankRight match {
        case Some(right) =>
          treeInsert(right, value)
        case None =>
          root.rankRight = Some(value)
      }
    }
  }

  /*
  Seize the rightmost child, editing this node or one of its sub-nodes to preserve the ordering.
   */
  @tailrec
  def seizeRightmost(root: MotifNode): MotifNode =
    root.rankRight match {
      case None => root
      case Some(r) =>
        r.rankRight match {
          case Some(rr) => seizeRightmost(r)
          case None =>
            root.rankRight = r.rankLeft
            r
        }
    }

  /*
  Seize the leftmost child, editing this node or one of its sub-nodes to preserve the ordering.
   */
  @tailrec
  def seizeLeftmost(root: MotifNode): MotifNode =
    root.rankLeft match {
      case None => root
      case Some(l) =>
        l.rankLeft match {
          case Some(ll) => seizeLeftmost(l)
          case None =>
            root.rankLeft = l.rankRight
            l
        }
    }

  /*
   * Delete a value that is known to exist in the tree, but is not equal to the root.
   */
  @tailrec
  def treeDelete(root: MotifNode, value: MotifNode) {
//    println(s"Delete $value root: $root")
    if (value < root) {
      if (root.rankLeft.get.rankSort == value.rankSort) {
        root.rankLeft = treeDeleteAt(root.rankLeft.get)
      } else {
        treeDelete(root.rankLeft.get, value)
      }
    } else { // value > root
      if (root.rankRight.get.rankSort == value.rankSort) {
        root.rankRight = treeDeleteAt(root.rankRight.get)
      } else {
        treeDelete(root.rankRight.get, value)
      }
    }
  }

  /*
   * Delete a value that has been found in the tree. Return the replacement for the node being deleted.
   */
  def treeDeleteAt(root: MotifNode): Option[MotifNode] = {
//    println(s"Delete at root: $root")
    if (root.rankRight != None) {
      val r = seizeLeftmost(root.rankRight.get)
      r.rankLeft = root.rankLeft
      if (Some(r) != root.rankRight) {
        r.rankRight = root.rankRight
      }
      Some(r)
    } else if (root.rankLeft != None) {
      val r = seizeRightmost(root.rankLeft.get)
      r.rankRight = root.rankRight
      if (Some(r) != root.rankLeft) {
        r.rankLeft = root.rankLeft
      }
      Some(r)
    } else {
      None
    }
  }

  def printTree(root: MotifNode, indent: String = ""): Unit = {
    println(s"$indent$root")
    for (l <- root.rankLeft) {
      printTree(l, indent + "L ")
    }
    for (r <- root.rankRight) {
      printTree(r, indent + "R ")
    }
  }

  @tailrec
  def treeLeftEdge(root: MotifNode, acc: List[MotifNode]): List[MotifNode] = {
    root.rankLeft match {
      case Some(l) => treeLeftEdge(l, root :: acc)
      case None => root :: acc
    }
  }

  def treeInOrder(root: MotifNode, acc: List[MotifNode] = Nil): List[MotifNode] = {
    val right = root.rankRight.map(r => treeInOrder(r, acc)).getOrElse(acc)
    root.rankLeft match {
      case Some(l) => treeInOrder(l, root :: right)
      case None => root :: right
    }
  }
}

final class TreeIterator(root: MotifNode) extends Iterator[Motif] {
  var at = treeLeftEdge(root, Nil)

  def hasNext = at != Nil

  def next: Motif = {
    val h = at.head
    h.rankRight match {
      case Some(r) =>
        at = treeLeftEdge(r, at.tail)
      case None =>
        at = at.tail
    }
    h.m
  }
}

/**
 * The PosRankList maintains two mutable doubly linked lists, corresponding to
 * position and rank ordering of items.
 *
 * This class is the start motif (highest (minimum) rank, lowest pos) for both lists,
 * and also the main public interface.
 */
final case class PosRankList() extends DLNode with Iterable[Motif] {
  import PosRankList._

  nextPos = Left(End())
  nextPos.left.get.prevPos = Left(this)

  var treeRoot: Option[MotifNode] = None

  def asForwardLink: Either[End, MotifNode] = ???
  def asBackwardLink = Left(this)

  def iterator: Iterator[Motif] = new Iterator[Motif] {
    var current = nextPos
    def hasNext = current.isRight
    def next = {
      val r = current.right.get
      current = current.right.flatMap(_.nextPos)
      r.m
    }
  }

  def rankIterator: Iterator[Motif] = {
    if (treeRoot == None) {
      Iterator.empty
    } else {
      new TreeIterator(treeRoot.get)
    }
  }


//    if (treeRoot.isEmpty) {
//      Iterator.empty
//    } else {
////      printTree(treeRoot.get)
////      println("In order: ")
////      println(treeInOrder(treeRoot.get))
//    }

  val end: End = nextPos.left.get

  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: MotifNode) {
    end.prevPos match {
      case Right(last) =>
        linkPos(last, mn, end)
      case Left(_) =>
        linkPos(this, mn, end)
    }

    if (treeRoot.isEmpty) {
      treeRoot = Some(mn)
    } else {
      treeInsert(treeRoot.get, mn)
    }
  }

  /**
   * Append a motif at the final position, and insert at the correct rank order.
   */
  def :+= (m: Motif) {
    this :+= MotifNode(m.pos, m)
  }

  /**
   * Take a specified number of highest ranking items.
   * Does not alter the list.
   */
  def takeByRank(n: Int): List[Motif] = {
    new RankListBuilder(this, n).build
  }

  /**
   * Removes items that can only be parsed
   * before the given sequence position, given the constraints
   * of the given MotifSpace (min permitted start offset, etc)
   */
  def dropUntilPosition(pos: Int, space: MotifSpace) {
    nextPos match {
      case Right(mn) => dropUntilPositionRec(mn, pos, space, this)
      case _ =>
    }
  }

  override def toString = {
    "PList(" + this.map(_.packedString).mkString(" ") + ")\n" +
    "  RList(" + this.rankIterator.map(_.packedString).mkString(" ") + ")\n"
  }
}

final class RankListBuilder(from: PosRankList, n: Int) {
  //Number of remaining items to generate for the resulting output list
  private[this] var rem: Int = n

  /**
   * Returns the resolved overlap when a and b are ordered.
   */
  def resolveOverlap(a: Motif, b: Motif, xs: List[Motif] = Nil): List[Motif] = {
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
  def resolveOverlap(before: Motif, x: Motif, after: Motif, xs: List[Motif]): List[Motif] = {
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
  def insertPosNoOverlap(into: List[Motif], ins: Motif): List[Motif] = {
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

  def build: List[Motif] = {
    val it = from.rankIterator
    if (it.isEmpty) {
      return Nil
    }
    rem = n - 1
    var r = it.next :: Nil
    while (!it.isEmpty && rem > 0) {
      val cur = it.next
      val nr = insertPosNoOverlap(r, cur)
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
final case class MotifNode(pos: Int, m: Motif) extends DLNode {
  import PosRankList._

  def asForwardLink = Right(this)
  def asBackwardLink = Right(this)

  var rankLeft: Option[MotifNode] = None
  var rankRight: Option[MotifNode] = None

  //NB this imposes a maximum length on analysed sequences for now
  lazy val rankSort = m.rankSort

  def < (other: MotifNode) =
    rankSort < other.rankSort

  /**
   * Remove this node from both of the two lists.
   */
  def remove(top: PosRankList) {
    prevPos.merge.nextPos = nextPos
    nextPos.merge.prevPos = prevPos

//    PosRankList.synchronized {
//      println("before:")
//      printTree(top.treeRoot.get)
      if (top.treeRoot.contains(this)) {
        top.treeRoot = treeDeleteAt(top.treeRoot.get)
      } else {
        treeDelete(top.treeRoot.get, this)
      }
//      println("after:")
//      printTree(top.treeRoot.get)
//    }
  }

  override def toString = s"$m [ $rankSort ]"
}

/**
 * Avoid recomputing takeByRank(n) by using a smart cache.
 */
final class TopRankCache(list: PosRankList, n: Int) {
  var cache: Option[List[Motif]] = None
  var firstByPos: Motif = _
  var lowestRank: Int = _
  var cacheLength: Int = 0

  def :+= (m: Motif) {
    list :+= m
    if (m.rankSort < lowestRank || cacheLength < n) {
      //Force recompute
      cache = None
    }
  }

  def dropUntilPosition(pos: Int, space: MotifSpace) {
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