package hypercut.hash

import scala.annotation.tailrec

sealed trait PositionNode {
  var prevPos: PositionNode = _  //PosRankList or MotifContainer
  var nextPos: PositionNode = _  // End or MotifContainer

  def remove(top: PosRankWindow): Unit = {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos
  }
  def isEnd: Boolean = false
}

trait MotifContainer extends PositionNode {
  def pos: Int
  def motif: Motif
}

/**
 * End marker for both lists
 */
final case class End() extends PositionNode {
  override def isEnd = true
}

object PosRankWindow {

  /**
   * Build a list from nodes with absolute positions.
   */
  def apply(nodes: Seq[Motif]) = {
    fromNodes(nodes.map(m => PositionRankNode(m.pos, m)))
  }

  /**
   * Build a list from position-sorted nodes.
   */
  def fromNodes(nodes: Seq[PositionRankNode]) = {
    val r = new PosRankWindow()

    for { n <- nodes } {
      r :+= n
    }
    r
  }

  def linkPos(before: PositionNode, middle: PositionNode, after: PositionNode) {
    before.nextPos = middle
    middle.prevPos = before
    middle.nextPos = after
    after.prevPos = middle
  }

  @tailrec
  def dropUntilPositionRec(from: MotifContainer, pos: Int, top: PosRankWindow) {
    if (from.pos < pos) {
      from.remove(top)
      from.nextPos match {
        case m: MotifContainer => dropUntilPositionRec(m, pos, top)
        case _ =>
      }
    }
  }

  @tailrec
  def treeInsert(root: PositionRankNode, value: PositionRankNode) {
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

  def treeSetLeft(parent: PositionRankNode, value: PositionRankNode): Unit = {
    parent.rankLeft = value
    if (value != null) {
      value.rankParent = parent
    }
  }

  def treeSetRight(parent: PositionRankNode, value: PositionRankNode): Unit = {
    parent.rankRight = value
    if (value != null) {
      value.rankParent = parent
    }
  }

  /*
  Seize the rightmost child, editing this node or one of its sub-nodes to preserve the ordering.
   */
  @tailrec
  def seizeRightmost(root: PositionRankNode): PositionRankNode = {
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
  def seizeLeftmost(root: PositionRankNode): PositionRankNode = {
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
  def treeDelete(immParent: PositionRankNode, value: PositionRankNode) {
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
  def treeDeleteAt(root: PositionRankNode): PositionRankNode = {
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

  def printTree(root: PositionRankNode, indent: String = ""): Unit = {
    println(s"$indent$root")
    for (l <- Option(root.rankLeft)) {
      printTree(l, indent + "L ")
    }
    for (r <- Option(root.rankRight)) {
      printTree(r, indent + "R ")
    }
  }

  def treeInOrder(root: PositionRankNode, acc: List[PositionRankNode] = Nil): List[PositionRankNode] = {
    val right = Option(root.rankRight).map(r => treeInOrder(r, acc)).getOrElse(acc)
    if (root.rankLeft != null) {
      treeInOrder(root.rankLeft, root :: right)
    } else root :: right
  }

  /*
  In-order traversal. Returns true while the traversal should continue.
  The supplied visitor receives values as they are seen and can interrupt the traversal.
   */
  def traverse(root: PositionRankNode, visitor: RankListBuilder): Boolean = {
    if (root.rankLeft != null) {
      if (!traverse(root.rankLeft, visitor)) return false
    }
    if (!visitor.treeVisitInOrder(root.motif)) return false
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
final case class PosRankWindow() extends PositionNode with Iterable[Motif] {
  import PosRankWindow._

  nextPos = End()
  nextPos.prevPos = this

  var treeRoot: Option[PositionRankNode] = None

  def iterator: Iterator[Motif] = new Iterator[Motif] {
    var current = nextPos
    def hasNext = current.isInstanceOf[MotifContainer]
    def next = {
      val r = current
      current = current.nextPos
      r.asInstanceOf[MotifContainer].motif
    }
  }

  //Inefficient implementation - traverse is preferred
  def rankIterator: Iterator[Motif] = {
    treeRoot match {
      case None => Iterator.empty
      case Some(r) => treeInOrder(r).iterator.map(_.motif)
    }
  }

  val end: End = nextPos.asInstanceOf[End]

  /**
   * Append at the final position,
   * inserting at the correct place in rank ordering.
   */
  def :+= (mn: PositionRankNode) {
    end.prevPos match {
      case last: MotifContainer =>
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
    this :+= PositionRankNode(m.pos, m)
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
  def dropUntilPosition(pos: Int) {
    nextPos match {
      case mc: MotifContainer => dropUntilPositionRec(mc, pos, this)
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
final case class PositionRankNode(pos: Int, motif: Motif) extends MotifContainer {
  import PosRankWindow._

  //Binary tree
  //Using null instead of Option for performance
  var rankLeft: PositionRankNode = null
  var rankRight: PositionRankNode = null
  var rankParent: PositionRankNode = null

  //NB this imposes a maximum length on analysed sequences for now
  val rankSort = motif.rankSort

  /**
   * Remove this node from both of the two sequences.
   */
  override def remove(top: PosRankWindow) {
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

  override def toString = s"$motif [ $rankSort ]"
}

/**
 * Smart cache to support repeated computation of takeByRank(n).
 */
trait TopRankCache {
  def :+= (m: Motif): Unit
  def dropUntilPosition(pos: Int): Unit
  def takeByRank: List[Motif]
}

object TopRankCache {
  def apply(n: Int): TopRankCache = {
    if (n > 1) {
      new FullTopRankCache(PosRankWindow(), n)
    } else {
      new FastTopRankCache
    }
  }
}

final class FullTopRankCache(list: PosRankWindow, n: Int) extends TopRankCache {
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
    list.dropUntilPosition(pos)
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
          list.dropUntilPosition(firstTopRankedByPos.pos)
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

/**
 * Optimised version of TopRankCache for the case n = 1
 */
final class FastTopRankCache extends TopRankCache {
  /*
   * The cache here is used for the position dimension only, and the rank dimension is ignored.
   *
   * Invariants: head of cache is top ranked, and also leftmost position.
   * Rank decreases (i.e. tagRank increases) monotonically going left to right.
   * Motifs are sorted by position.
   */
  val cache = new PosRankWindow
  var lastRes: List[Motif] = Nil
  var lastResPos: Int = 0
  var lastResRank: Int = Int.MaxValue

  /**
   * Walk the list from the end (lowest priority/high tagRank)
   * ensuring monotonicity.
   */
  @tailrec
  def ensureMonotonic(from: MotifContainer): Unit = {
    from.prevPos match {
      case mc: MotifContainer =>
        if (from.motif.features.tagRank < mc.motif.features.tagRank) {
          mc.remove(cache)
          ensureMonotonic(from)
        }
      case _ =>
    }
  }

  /**
   * Search the list from the end, inserting a new element and possibly
   * dropping a previously inserted suffix in the process.
   * @param insert
   * @param search
   */
  @tailrec
  def appendMonotonic(insert: Motif, search: PositionNode): Unit = {
    search.prevPos match {
      case mc: MotifContainer =>
        val mcr = mc.motif.features.tagRank
        if (insert.motif.features.tagRank < mcr) {
          //Drop mc
          appendMonotonic(insert, mc)
        } else {
          //found the right place, insert here and cause other elements to be dropped
          PosRankWindow.linkPos(mc, insert, cache.end)
        }
      case x =>
        PosRankWindow.linkPos(x, insert, cache.end)
    }
  }

  def :+= (m: Motif): Unit = {
    if (m.features.tagRank < lastResRank) {
      //new item is the highest priority one
      lastRes = Nil
      //wipe pre-existing elements from the cache
      PosRankWindow.linkPos(cache, m, cache.end)
    } else {
      appendMonotonic(m, cache.end)
    }
  }

  def dropUntilPosition(pos: Int): Unit = {
    cache.dropUntilPosition(pos)
    if (pos > lastResPos) {
      lastRes = Nil
    }
  }

  def takeByRank: List[Motif] = {
    if (! (lastRes eq Nil)) {
      lastRes
    } else {
      cache.nextPos match {
        case mc: MotifContainer =>
          lastRes = mc.motif :: Nil
          lastResPos = mc.pos
          lastResRank = mc.motif.features.tagRank
          lastRes
        case _ => Nil
      }
    }
  }
}