package dbpart.bucket

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
    val withKeys = items.map(i => (i, keys(i)))

    withKeys.foldLeft(List[WithKeys[A, K]]())((acc, i) => {
      val ks = i._2.to[MSet]
      unify(List(i._1), ks, acc, keys)
    }).map(_._1)
  }

  /**
   * Iterate through clusters, unifying them
   * @param from list to unify with as many clusters as possible
   * @param fromKeys keys of from
   * @param clusters iterate through this list of existing clusters
   * @param keys key construction function
   * @param acc result accumulator
   * @tparam A
   * @tparam K
   * @return
   */
  @tailrec
  def unify[A, K](from: List[A], fromKeys: CSet[K],
                  clusters: List[WithKeys[A, K]],
                  keys: A => Iterable[K], acc: List[WithKeys[A, K]] = Nil): List[WithKeys[A, K]] = {
    clusters match {
      case (l, ks) :: ls =>
        if (ks.iterator.filter(fromKeys.contains).nonEmpty) {
          unify(l ::: from, fromKeys ++ ks, ls, keys, acc)
        } else {
          unify(from, fromKeys, ls, keys, (l, ks) :: acc)
        }
      case _ => (from, fromKeys.toSeq):: acc
    }
  }
}