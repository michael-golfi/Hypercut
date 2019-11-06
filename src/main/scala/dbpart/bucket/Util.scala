package dbpart.bucket

import com.github.pathikrit.scalgos.DisjointSet

import scala.collection.{Set => CSet}
import scala.collection.mutable.{Set => MSet}
import scala.annotation.tailrec

object Util {

  type WithKeys[A, K] = (List[A], Iterable[K])

  /**
   * Given a collection and a function that produces multiple keys from each item,
   * produces a partitioning such that items are in the same partition iff they
   * share at least one key.
   * Items are expected to be distinct.
   */
  def partitionByKeys[A, K](items: List[A], keys: A => Iterable[K]): List[List[A]] = {
    val disjoint = DisjointSet.empty[K, A]
    for {
      i <- items
      ks = keys(i)
      head <- ks.headOption
    } {
      if (!disjoint.contains(head)) {
        disjoint += head
      }
      disjoint.assignData(head, i)
      for {
        k <- ks
        if !disjoint.contains(k)
      } {
        disjoint += k
      }
    }

    for {
      i <- items
      ks = keys(i)
      head <- ks.headOption
      k <- ks
    } {
      disjoint.union(head, k)
    }
    val partitioned = disjoint.setAnnotations.map(_.toList)
    partitioned.toList
  }


}