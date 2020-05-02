package hypercut.hash

import scala.util.Either.MergeableEither
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * A node that participates in two mutable doubly linked lists:
 * One sorted by rank, one sorted by position.
 */
sealed trait DLNode {
  type Backward = DLNode //PosRankList or MotifNode
  type Forward = DLNode // End or MotifNode
  var prevPos: Backward = _
  var nextPos: Forward = _

}

/**
 * End marker for both lists
 */
final case class End() extends DLNode {

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
    before.nextPos = middle
    middle.prevPos = before
    middle.nextPos = after
    after.prevPos = middle
  }

  @tailrec
  def dropUntilPositionRec(from: MotifNode, pos: Int, space: MotifSpace, top: PosRankList) {
    if (from.pos < pos + space.minPermittedStartOffset(from.m.features.tag)) {
      from.remove(top)
      from.nextPos match {
        case m: MotifNode => dropUntilPositionRec(m, pos, space, top)
        case _ =>
      }
    }
  }

  @tailrec
  def treeInsert(root: MotifNode, value: MotifNode) {
    if (value.rankSort < root.rankSort) {
      if (root.rankLeft != null) {
        treeInsert(root.rankLeft, value)
      } else {
        root.rankLeft = value
      }
    } else {
      if (root.rankRight != null) {
        treeInsert(root.rankRight, value)
      } else {
        root.rankRight = value
      }
    }
  }

  /*
  Seize the rightmost child, editing this node or one of its sub-nodes to preserve the ordering.
   */
  @tailrec
  def seizeRightmost(root: MotifNode): MotifNode = {
    val r = root.rankRight
    if (r == null) root else {
      if (r.rankRight != null) {
        seizeRightmost(r)
      } else {
        root.rankRight = r.rankLeft
        r
      }
    }
  }

  /*
  Seize the leftmost child, editing this node or one of its sub-nodes to preserve the ordering.
   */
  @tailrec
  def seizeLeftmost(root: MotifNode): MotifNode = {
    val l = root.rankLeft
    if (l == null) root else {
      if (l.rankLeft != null) {
        seizeLeftmost(l)
      } else {
        root.rankLeft = l.rankRight
        l
      }
    }
  }

  /*
   * Delete a value that is known to exist in the tree, but is not equal to the root.
   */
  @tailrec
  def treeDelete(root: MotifNode, value: MotifNode) {
//    println(s"Delete $value root: $root")
    if (value.rankSort < root.rankSort) {
      if (root.rankLeft.rankSort == value.rankSort) {
        root.rankLeft = treeDeleteAt(root.rankLeft)
      } else {
        treeDelete(root.rankLeft, value)
      }
    } else { // value > root
      if (root.rankRight.rankSort == value.rankSort) {
        root.rankRight = treeDeleteAt(root.rankRight)
      } else {
        treeDelete(root.rankRight, value)
      }
    }
  }

  /*
   * Delete a value that has been found in the tree.
   * Return the replacement for the node being deleted (possibly null).
   */
  def treeDeleteAt(root: MotifNode): MotifNode = {
//    println(s"Delete at root: $root")
    if (root.rankRight != null) {
      val r = seizeLeftmost(root.rankRight)
      r.rankLeft = root.rankLeft
      if (r != root.rankRight) {
        r.rankRight = root.rankRight
      }
      r
    } else if (root.rankLeft != null) {
      val r = seizeRightmost(root.rankLeft)
      r.rankRight = root.rankRight
      if (r != root.rankLeft) {
        r.rankLeft = root.rankLeft
      }
      r
    } else {
      null
    }
  }

  def printTree(root: MotifNode, indent: String = ""): Unit = {
    println(s"$indent$root")
    for (l <- Option(root.rankLeft)) {
      printTree(l, indent + "L ")
    }
    for (r <- Option(root.rankRight)) {
      printTree(r, indent + "R ")
    }
  }

  def treeInOrder(root: MotifNode, acc: List[MotifNode] = Nil): List[MotifNode] = {
    val right = Option(root.rankRight).map(r => treeInOrder(r, acc)).getOrElse(acc)
    if (root.rankLeft != null) {
      treeInOrder(root.rankLeft, root :: right)
    } else root :: right
  }

  /*
  In-order traversal. Returns true while the traversal should continue.
  The supplied visitor receives values as they are seen and can interrupt the traversal.
   */
  def traverse(root: MotifNode, visitor: RankListBuilder): Boolean = {
    if (root.rankLeft != null) {
      if (!traverse(root.rankLeft, visitor)) return false
    }
    if (!visitor.treeVisitInOrder(root.m)) return false
    if (root.rankRight != null) {
      traverse(root.rankRight, visitor)
    } else true
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

  nextPos = End()
  nextPos.prevPos = this

  var treeRoot: Option[MotifNode] = None

  def iterator: Iterator[Motif] = new Iterator[Motif] {
    var current = nextPos
    def hasNext = current.isInstanceOf[MotifNode]
    def next = {
      val r = current
      current = current.nextPos
      r.asInstanceOf[MotifNode].m
    }
  }

  //Inefficient implementation - traverse is preferred
  def rankIterator: Iterator[Motif] = {
    treeRoot match {
      case None => Iterator.empty
      case Some(r) => treeInOrder(r).iterator.map(_.m)
    }
  }

  val end: End = nextPos.asInstanceOf[End]

  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: MotifNode) {
    end.prevPos match {
      case last: MotifNode =>
        linkPos(last, mn, end)
      case _ =>
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
      case mn: MotifNode => dropUntilPositionRec(mn, pos, space, this)
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

  var building: List[Motif] = Nil
  def treeVisitInOrder(node: Motif): Boolean = {
    building = insertPosNoOverlap(building, node)
    rem > 0
  }

  def build: List[Motif] = {
    if (from.treeRoot.isEmpty) return Nil
    PosRankList.traverse(from.treeRoot.get, this)
    building
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

  //Using null instead of Option for performance
  var rankLeft: MotifNode = null
  var rankRight: MotifNode = null

  //NB this imposes a maximum length on analysed sequences for now
  val rankSort = m.rankSort

  /**
   * Remove this node from both of the two lists.
   */
  def remove(top: PosRankList) {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos

//    PosRankList.synchronized {
//      println("before:")
//      printTree(top.treeRoot.get)
      if (top.treeRoot.contains(this)) {
        top.treeRoot = Option(treeDeleteAt(top.treeRoot.get))
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
          firstByPos = r.head
          cacheLength = 0
          lowestRank = 0
          inspectCache(r)
        }
        r
    }
  }

  @tailrec
  def inspectCache(data: List[Motif]): Unit = {
    if (!data.isEmpty) {
      val h = data.head
      cacheLength += 1
      if (h.rankSort > lowestRank) {
        lowestRank = h.rankSort
      }
      inspectCache(data.tail)
    }
  }
}