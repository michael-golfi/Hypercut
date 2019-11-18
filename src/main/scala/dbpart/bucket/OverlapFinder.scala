package dbpart.bucket

import dbpart.bucket.BoundaryBucket._
import scala.collection.mutable.{Set => MSet, Map => MMap}

/**
 * A class that simplifies repeated checking for overlaps against some collection of k-mers.
 */
final class OverlapFinder(val preAndSuf: Array[String], val pre: Array[String], val suf: Array[String],
                         k: Int) {
  import dbpart.bucket.Util._

  @transient
  val prefixAndSuffix = preAndSuf.to[MSet]
  @transient
  val prefix = pre.to[MSet]
  @transient
  val suffix = suf.to[MSet]

  def find(other: Iterable[String]): Iterator[String] = {
    val otherPreSuf = prefixesAndSuffixes(other.iterator, k)
    val otherPre = purePrefixes(other.iterator, k)
    val otherSuf = pureSuffixes(other.iterator, k)

    find(otherPreSuf, otherPre, otherSuf)
  }

  def check(other: Iterable[String]): Boolean = {
    find(other).nonEmpty
  }

  def check(other: String): Boolean =
    check(Seq(other))

  def find(other: OverlapFinder): Iterator[String] =
    find(other.prefixAndSuffix.iterator, other.prefix.iterator, other.suffix.iterator)

  def check(other: OverlapFinder): Boolean =
    find(other).nonEmpty

  def find(othPreSuf: Iterator[String], othPre: Iterator[String], othSuf: Iterator[String]): Iterator[String] = {
    othPreSuf.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x) || prefix.contains(x)) ++
      othPre.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x)) ++
      othSuf.filter(x => prefixAndSuffix.contains(x) || prefix.contains(x))
  }

  def check(othPreSuf: Iterator[String], othPre: Iterator[String], othSuf: Iterator[String]): Boolean =
    find(othPreSuf, othPre, othSuf).nonEmpty

  def suffixes(othPreSuf: Iterator[String], othPre: Iterator[String]): Iterator[String] = {
    othPreSuf.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x)) ++
      othPre.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x))
  }

  def prefixes(othPreSuf: Iterator[String], othSuf: Iterator[String]): Iterator[String] = {
    othPreSuf.filter(x => prefixAndSuffix.contains(x) || prefix.contains(x)) ++
      othSuf.filter(x => prefixAndSuffix.contains(x) || prefix.contains(x))
  }
}