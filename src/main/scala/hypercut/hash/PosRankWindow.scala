package hypercut.hash

import scala.annotation.tailrec

sealed trait PRNode {
  var prevPos: PRNode = _  //PosRankList or MotifNode
  var nextPos: PRNode = _  // End or MotifNode
}

/**
 * End marker for both lists
 */
final case class End() extends PRNode

object PosRankWindow {

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
    val r = new PosRankWindow()

    for { n <- nodes } {
      r :+= n
    }
    r
  }

  def linkPos(before: PRNode, middle: MotifNode, after: PRNode) {
    before.nextPos = middle
    middle.prevPos = before
    middle.nextPos = after
    after.prevPos = middle
  }

  @tailrec
  def dropUntilPositionRec(from: MotifNode, pos: Int, space: MotifSpace, top: PosRankWindow) {
    if (from.pos < pos) {
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
        treeSetLeft(root, value)
      }
    } else {
      if (root.rankRight != null) {
        treeInsert(root.rankRight, value)
      } else {
        treeSetRight(root, value)
      }
    }
  }

  def treeSetLeft(parent: MotifNode, value: MotifNode): Unit = {
    parent.rankLeft = value
    if (value != null) {
      value.rankParent = parent
    }
  }

  def treeSetRight(parent: MotifNode, value: MotifNode): Unit = {
    parent.rankRight = value
    if (value != null) {
      value.rankParent = parent
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
        treeSetRight(root, r.rankLeft)
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
        treeSetLeft(root, l.rankRight)
        l
      }
    }
  }

  /*
   * Delete either the right or left child of a node
   */
  def treeDelete(immParent: MotifNode, value: MotifNode) {
//    println(s"Delete $value root: $root")
    if (value eq immParent.rankLeft) {
      treeSetLeft(immParent, treeDeleteAt(immParent.rankLeft))
    } else {
      //right child should be what we are deleting
      treeSetRight(immParent, treeDeleteAt(immParent.rankRight))
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
      treeSetLeft(r, root.rankLeft)
      if (r != root.rankRight) {
        treeSetRight(r, root.rankRight)
      }
      r
    } else if (root.rankLeft != null) {
      val r = seizeRightmost(root.rankLeft)
      treeSetRight(r, root.rankRight)
      if (r != root.rankLeft) {
        treeSetLeft(r, root.rankLeft)
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
 * The PosRankWindow maintains two sequences, one implemented using a mutable doubly linked list,
 * the other implemented using a tree, corresponding to position and rank ordering of items.
 *
 * This class is the start motif (highest (minimum) rank, lowest pos) for both lists,
 * and also the main public interface.
 */
final case class PosRankWindow() extends PRNode with Iterable[Motif] {
  import PosRankWindow._

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

    //isEmpty rather than pattern match for efficiency
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
    "PRWin(" + this.map(_.packedString).mkString(" ") + ")\n" +
    "  PRWin(" + this.rankIterator.map(_.packedString).mkString(" ") + ")\n"
  }
}

/**
 * Utility class to efficiently pick the n top ranked PRNodes from a PosRankWindow.
 */
final class RankListBuilder(from: PosRankWindow, n: Int) {
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
    PosRankWindow.traverse(from.treeRoot.get, this)
    building
  }
}

/**
 * A node that participates in the two sequences and contains a motif.
 * Pos is absolute position.
 */
final case class MotifNode(pos: Int, m: Motif) extends PRNode {
  import PosRankWindow._

  //Binary tree
  //Using null instead of Option for performance
  var rankLeft: MotifNode = null
  var rankRight: MotifNode = null
  var rankParent: MotifNode = null

  //NB this imposes a maximum length on analysed sequences for now
  val rankSort = m.rankSort

  /**
   * Remove this node from both of the two sequences.
   */
  def remove(top: PosRankWindow) {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos

//    PosRankWindow.synchronized {
//      println("before:")
//      printTree(top.treeRoot.get)
      if (top.treeRoot.get eq this) {
        top.treeRoot = Option(treeDeleteAt(top.treeRoot.get))
      } else {
        treeDelete(this.rankParent, this)
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
final class TopRankCache(space: MotifSpace, list: PosRankWindow, n: Int) {
  var cache: Option[List[Motif]] = None
  var firstTopRankedByPos: Motif = _
  var lowestRank: Int = _
  var cacheLength: Int = 0

  def :+= (m: Motif) {
    list :+= m
    if (m.rankSort < lowestRank || cacheLength < n) {
      //Force recompute since we know that the cached data might now be invalid
      cache = None
    }
  }

  def dropUntilPosition(pos: Int) {
    list.dropUntilPosition(pos, space)
    if (firstTopRankedByPos != null && pos > firstTopRankedByPos.pos) {
      //Force recompute, since we have dropped part of the result
      cache = None
    }
  }

  /**
   * Obtain the current top ranked motifs. Also alters the list by removing motifs
   * deemed to be irrelevant at the current position.
   */
  def takeByRank: List[Motif] = {
    cache match {
      case Some(c) => c
      case _ =>
        val r = list.takeByRank(n)
        if (!r.isEmpty) {
          cache = Some(r)
          firstTopRankedByPos = r.head
          cacheLength = 0
          lowestRank = 0
          //At this point, we know that the preceding range cannot participate in any future
          //top ranked sets
          list.dropUntilPosition(firstTopRankedByPos.pos, space)
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