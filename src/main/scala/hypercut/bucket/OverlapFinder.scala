package hypercut.bucket

import com.google.common.hash.{BloomFilter, Funnels}

import scala.collection.mutable.{Map => MMap, Set => MSet}
import hypercut.bucket.Util._

/**
 * A class that simplifies repeated checking for overlaps against some collection of k-mers.
 * @param preAndSuf (k-1)-mers in prefix/suffix position
 * @param pre (k-1)-mers in prefix position
 * @param suf (k-1)-mers in suffix position
 */
final class SetOverlapFinder(val preAndSuf: Array[String], val pre: Array[String], val suf: Array[String],
                         k: Int) {
  val prefixAndSuffix = preAndSuf.to[MSet]
  val prefix = pre.to[MSet]
  val suffix = suf.to[MSet]

  def find(other: Iterable[String]): Iterator[String] = {
    val otherPreSuf = prefixesAndSuffixes(other.iterator, k)
    val otherPre = purePrefixes(other.iterator, k)
    val otherSuf = pureSuffixes(other.iterator, k)

    find(otherPreSuf, otherPre, otherSuf)
  }

  def check(other: Iterable[String]): Boolean =
    find(other).nonEmpty

  def check(other: String): Boolean =
    check(Seq(other))

  def find(othPreSuf: Iterator[String], othPre: Iterator[String], othSuf: Iterator[String]): Iterator[String] = {
    othPreSuf.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x) || prefix.contains(x)) ++
      othPre.filter(x => prefixAndSuffix.contains(x) || suffix.contains(x)) ++
      othSuf.filter(x => prefixAndSuffix.contains(x) || prefix.contains(x))
  }
}

/**
 * A class that simplifies repeated checking for overlaps against some collection of k-mers.
 * This version associates multiple potential overlaps with an ID annotations.
 *
 * @param preAndSuf (k-1)-mers in prefix/suffix position, paired with corresponding IDs
 * @param pre (k-1)-mers in prefix position, paired with corresponding IDs
 * @param suf (k-1)-mers in suffix position, paired with corresponding IDs
 */
final class MapOverlapFinder(val preAndSuf: Seq[IdChunk], val pre: Seq[IdChunk], val suf: Seq[IdChunk],
                             k: Int) {
  /**
   * Maps each (k-1)-mer to IDs that it belongs to
   */
  def buildMap(chunks: Seq[IdChunk]): MMap[String, List[Long]] = {
    val r = MMap.empty[String, List[Long]]
    for {
      (seqs, id) <- chunks
      s <- seqs
    } {
      r.get(s) match {
        case Some(exist) =>
          r += (s -> (id :: exist).distinct)
        case None =>
          r += s -> List(id)
      }
    }
    r
  }

  val prefixAndSuffix = buildMap(preAndSuf)
  val prefix = buildMap(pre)
  val suffix = buildMap(suf)

  def preAndSufGet(x: String) = prefixAndSuffix.getOrElse(x, Nil).iterator.
    map(id => (id, x))
  def preGet(x: String) = prefix.getOrElse(x, Nil).iterator.
    map(id => (id, x))
  def sufGet(x: String) = suffix.getOrElse(x, Nil).iterator.
    map(id => (id, x))

  def find(other: Iterable[String]): List[IdChunk] = {
    val otherPreSuf = prefixesAndSuffixes(other.iterator, k)
    val otherPre = purePrefixes(other.iterator, k)
    val otherSuf = pureSuffixes(other.iterator, k)

    findIdChunks(otherPreSuf, otherPre, otherSuf)
  }

  def find(othPreSuf: Iterator[String], othPre: Iterator[String], othSuf: Iterator[String]): Iterator[(Long, String)] = {
    othPreSuf.flatMap(x => preAndSufGet(x) ++ sufGet(x) ++ preGet(x)) ++
      othPre.flatMap(x => preAndSufGet(x) ++ sufGet(x)) ++
      othSuf.flatMap(x => preAndSufGet(x) ++ preGet(x))
  }

  def findIdChunks(othPreSuf: Iterator[String], othPre: Iterator[String], othSuf: Iterator[String]):
    List[IdChunk] =
    find(othPreSuf, othPre, othSuf).toList.groupBy(_._1).
      values.toList.map(g => (g.map(_._2).toArray, g.head._1))

  // Find against a bloom filter.
  // Two sources of false positives:
  // 1. The nature of the bloom filter itself
  // 2. The bloom filter doesn't distinguish prefix/suffix/both like this class does.
  // Thus a suffix here might match against a suffix there, which is not a true overlap.
  def find(other: BloomBucket): List[IdChunk] = {
    val allChunks = prefixAndSuffix.iterator ++ prefix.iterator ++ suffix.iterator
    val all = for {
      (data, ids) <- allChunks
      if other.mightContain(data)
      id <- ids
      result = (id, data)
    } yield result
    val grouped = all.toList.groupBy(_._1).values
    grouped.toList.map(g => (g.map(_._2).toArray, g.head._1))
  }
}

object BloomFilterOverlap {

  def filterFromSequences(data: Array[String], k: Int): BloomBucket = {
    val nElements = data.map(_.length - (k - 2)).sum
    val falsePositiveRate = 0.001
    val bf = BloomFilter.create(Funnels.stringFunnel(), nElements, falsePositiveRate)
    for (preOrSuf <- prefixesAndSuffixes(data.iterator, k)) {
      bf.put(preOrSuf)
    }
    bf
  }
}