package dbpart.bucket

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

object Util {

  /**
   * Given a collection and a function that produces multiple keys from each item,
   * produces a partitioning such that items are in the same partition iff they
   * share at least one key.
   */
  def partitionByKeys[A, K](items: List[A], keys: A => Iterable[K]): List[List[A]] = {
    var r = List[List[A]]()
    for {
      i <- items
      ks = keys(i).toSeq
    } {
      r = unify(List(i), ks, r, keys)
    }
    r
  }

  @tailrec
  def unify[A, K](from: List[A], fromKeys: Seq[K],
      lists: List[List[A]], keys: A => Iterable[K], acc: List[List[A]] = Nil): List[List[A]] = {
    lists match {
      case l :: ls =>
        val ks = l.flatMap(keys)
        if (fromKeys.intersect(ks).nonEmpty) {
          unify(l ::: from, fromKeys, ls, keys, acc)
        } else {
          unify(from, fromKeys, ls, keys, l :: acc)
        }
      case _ => from :: acc
    }
  }
}