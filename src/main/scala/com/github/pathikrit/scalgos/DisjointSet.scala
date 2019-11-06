
package com.github.pathikrit.scalgos

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.language.implicitConversions


/**
 * Adapted from
 * https://github.com/pathikrit/scalgos/blob/master/src/main/scala/com/github/pathikrit/scalgos/DisjointSet.scala
 *
 * A disjoint-set data structure (also called union-find data structure)
 * Has efficient union and find operations in amortised O(a(n)) time (where a is the inverse-Ackermann function)
 * TODO: Support delete
 * TODO: extend scala collection
 *
 * @tparam A types of things in set
 * @tparam Annot Data annotation for sets
 *
 * @author Pathikrit Bhowmick
 */

class DisjointSet[A, Annot] {
  import DisjointSet.Node
  private[this] val parent = mutable.Map.empty[A, Node[A, Annot]]

  private[this] implicit def toNode(x: A) = {
    assume(contains(x))
    parent(x)
  }

  /**
   * @return true iff x is known
   */
  def contains(x: A) = parent contains x

  /**
   * Add a new singleton set with only x in it (assuming x is not already known)
   */
  def +=(x: A) = {
    assume(!contains(x))
    parent(x) = new Node(x)
  }

  /**
   * Union the sets containing x and y
   */
  def union(x: A, y: A) = {
    val (xRoot, yRoot) = (x.root, y.root)
    if (xRoot != yRoot) {
      if (xRoot.rank < yRoot.rank) {        // change the root of the shorter/less-depth one
        xRoot.parent = yRoot
        yRoot.annotation = yRoot.annotation ++ xRoot.annotation
      } else if (xRoot.rank > yRoot.rank) {
        yRoot.parent = xRoot
        xRoot.annotation = yRoot.annotation ++ xRoot.annotation
      } else {
        yRoot.parent = xRoot
        xRoot.annotation = yRoot.annotation ++ xRoot.annotation
        xRoot.rank += 1   // else if there is tie, increment
      }
    }
  }

  /**
   * @return the root (or the canonical element that contains x)
   */
  def apply(x: A) = x.root.entry

  /**
   * @return Iterator over groups of items in same set
   */
  def sets = parent.keys groupBy {_.root.entry} values

  /**
   * @param x
   * @param d
   */
  def assignData(x: A, d: Annot): Unit = {
    x.root.annotation = (x.root.annotation :+ d).distinct
  }

  def setAnnotations = parent.keys.map(_.root).toSeq.distinct.map(_.annotation)
}

object DisjointSet {
  /**
   * Each internal node in DisjointSet
   */
  private[DisjointSet] class Node[A, Annot](val entry: A) {
    /**
     * parent - the pointer to root node (by default itself)
     * rank - depth if we did not do path compression in find - else its upper bound on the distance from node to parent
     */
    var (parent, rank) = (this, 0)

    def root: Node[A, Annot] = {
      if (parent != this) {
        parent = parent.root     // path compression
      }
      parent
    }

    var annotation: ArrayBuffer[Annot] = ArrayBuffer.empty
  }

  /**
   * @return empty disjoint set
   */
  def empty[A, Annot] = new DisjointSet[A, Annot]

  /**
   * @return a disjoint set with each element in its own set
   */
  def apply[A, Annot](elements: A*) = {
    val d = empty[A, Annot]
    elements foreach {e => d += e}
    d
  }
}