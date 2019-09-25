package dbpart.bucket

import dbpart._
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers
import dbpart.graph.PathGraphBuilder
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder
import scala.collection.immutable.SortedSet
import scala.collection.{ Searching, Set => CSet }
import scala.collection.mutable.{ Map => MMap, Set => MSet }
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object BoundaryBucket {
  /**
   * Whether two sorted lists have at least one common element
   */
  @tailrec
  def haveIntersection(d1: List[String], d2: List[String]): Boolean = {
    d1 match {
      case Nil => false
      case x :: xs => d2 match {
        case Nil => false
        case y :: ys =>
          val c = x.compareTo(y)
          if (c < 0) {
            haveIntersection(xs, y :: ys)
          } else if (c == 0) {
            true
          } else {
            haveIntersection(x :: xs, ys)
          }
      }
    }
  }

  def prefixesAndSuffixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      //Number of (k-1)-mers
      val num = s.length() - (k - 2)
      Read.kmers(s, k - 1).drop(1).take(num - 2)
    })

  def purePrefixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      if (s.length >= k - 1) {
        Some(DNAHelpers.kmerPrefix(s, k))
      } else {
        None
      }
    })

  def pureSuffixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      if (s.length >= k - 1) {
        Some(DNAHelpers.kmerSuffix(s, k))
      } else {
        None
      }
    })

  def suffixes(ss: Iterable[String], k: Int) =
    pureSuffixes(ss.iterator, k) ++ prefixesAndSuffixes(ss.iterator, k)

  def prefixes(ss: Iterable[String], k: Int) =
    purePrefixes(ss.iterator, k) ++ prefixesAndSuffixes(ss.iterator, k)

  /**
   * Function for computing shared k-1 length overlaps between sequences of prior
   * and post k-mers. Prior gets cached as a set.
   */
  def sharedOverlapsThroughPrior(prior: Iterable[String], post: Iterable[String], k: Int): Iterator[String] = {
    val preKmers = suffixes(prior, k).to[MSet]
    val postKmers = prefixes(post, k)
    postKmers.filter(preKmers.contains)
  }

  /**
   * As above, but post gets cached as a set.
   */
  def sharedOverlapsThroughPost(prior: Iterable[String], post: Iterable[String], k: Int): Iterator[String] = {
    val preKmers = suffixes(prior, k)
    val postKmers = prefixes(post, k).to[MSet]
    preKmers.filter(postKmers.contains)
  }

  def overlapFinder(data: Iterable[String], k: Int) =
    new OverlapFinder(prefixesAndSuffixes(data.iterator, k).to[MSet],
      purePrefixes(data.iterator, k).to[MSet],
      pureSuffixes(data.iterator, k).to[MSet],
      k)

  /**
   * A class that simplifies repeated checking for overlaps against some collection of k-mers.
   */
  class OverlapFinder(val prefixAndSuffix: CSet[String], val prefix: CSet[String], val suffix: CSet[String],
                      k: Int) {

    def find(other: Iterable[String]): Iterator[String] = {
      val otherPreSuf = prefixesAndSuffixes(other.iterator, k)
      val otherPre = purePrefixes(other.iterator, k)
      val otherSuf = pureSuffixes(other.iterator, k)

      find(otherPreSuf, otherPre, otherSuf)
    }

    def check(other: Iterable[String]): Boolean = {
      find(other).nonEmpty
    }

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

  /**
   * Split sequences into maximal groups that are not connected
   * by any k-1 overlaps.
   */
  def splitSequences(ss: Seq[String], k: Int): List[List[String]] = {
    Util.partitionByKeys(ss.toList, (s: String) => Read.kmers(s, k - 1).toList)
  }

  /**
   * Unify pairs of ID and sequences where the ID sequences have
   * intersection.
   */
  def unifyParts(parts: List[(List[Long], List[String])]) = {
    val unified = Util.partitionByKeys(parts, (x: (List[Long], List[String])) => x._1)
    unified.map(u => {
      val (ks, vs) = (u.flatMap(_._1).distinct, u.flatMap(_._2))
      (vs.toArray, ks)
    })
  }

  /**
   * From a list of k-mers, remove any k-mers contained in
   * the iterator of (potentially long) existing sequences, and join the iterator back into strings as much as possible.
   */
  def removeKmers(toRemove: Iterator[NTSeq], removeFrom: List[NTSeq], k: Int): List[String] = {
    val removeSet = toRemove.flatMap(Read.kmers(_, k)).to[MSet]
    removeKmers(removeSet, removeFrom, k)
  }

  /**
   * Remove k-mers
   * @param toRemove k-mers to remove
   * @param removeFrom long sequences that will be filtered
   * @param k
   * @return
   */
  def removeKmers(toRemove: CSet[NTSeq], removeFrom: List[NTSeq], k: Int): List[String] =
    Read.flattenKmers(removeFrom.filter(s => !toRemove.contains(s)), k, Nil)

  /**
   * Remove k-mers
   * @param toRemove long sequences to remove
   * @param removeFrom long sequences that will be filtered
   * @param k
   * @return
   */
  def removeKmers(toRemove: Iterable[NTSeq], removeFrom: Iterable[NTSeq], k: Int): Seq[String] =
    removeKmers(toRemove.iterator, removeFrom.flatMap(Read.kmers(_, k)).toList, k)

  //Contigs ready for output, new buckets, relabelled IDs, edges to be removed (in terms of old IDs)
  type MergedBuckets = (List[Contig], List[(Array[String], List[Long])])

  /**
   * Remove unitigs from the core (whose boundary is known)
   * and merge it with its boundary buckets when there is still a shared (k-1)-mer
   * after unitig removal.
   */
  def seizeUnitigsAndMerge(
    core:     BoundaryBucket,
    boundary: List[BoundaryBucket]): MergedBuckets = {
    val (unitigs, updated) = core.seizeUnitigs(boundary.flatMap(_.core))

    //Split the core into parts that have no mutual overlap
    val split = updated.splitSequences

    val splitFinders = split.map(s => (s, overlapFinder(s, core.k)))


    //Pair each split part with the boundary IDs that it intersects with

    //On the assumption that buckets maintain roughly equal size,
    //it is cheaper to retain finders for s (bucket subparts) and traverse
    //the boundary (full buckets) rather than the opposite
    val swb = boundary.map(b => {
      val bfinder = b.overlapFinder
      splitFinders.map { case (s, sfinder) => {
        if (bfinder.check(sfinder)) {
          Some(b.id)
        } else {
          None
        }
      } }
    })
    val bySplit = swb.transpose

    val splitWithBoundary = bySplit.map(_.flatten) zip split

    //Split parts that had no intersection with the boundary will be output as
    //unitigs at this stage, so OK to drop them

    val mergedParts = unifyParts(splitWithBoundary)

    val intersectParts = mergedParts.map(m => {
      val fromCore = m._1
      (fromCore, m._2)
    })

    (unitigs, intersectParts)
  }
}

case class BoundaryBucket(id: Long, core: Array[String], k: Int) {
  import BoundaryBucket._

  def kmers = core.flatMap(Read.kmers(_, k))

  def coreStats = BucketStats(core.size, 0, kmers.size)

  def pureSuffixSet: CSet[String] = pureSuffixes(core.iterator, k).to[MSet]

  def purePrefixSet: CSet[String] = purePrefixes(core.iterator, k).to[MSet]

  def prefixAndSuffixSet: CSet[String] = prefixesAndSuffixes(core.iterator, k).to[MSet]

  def overlapFinder = new OverlapFinder(prefixAndSuffixSet, purePrefixSet,
    pureSuffixSet, k)

  import Searching._

  def nodesForGraph(boundary: Iterable[String]) = {
    //Possible optimization: try to do this with boundary as a simple iterator instead

    //boundary is assumed to potentially be much larger than core.
    val in = sharedOverlapsThroughPost(boundary, core, k).toArray.sorted
    val out = sharedOverlapsThroughPrior(core, boundary, k).toArray.sorted

    //TODO is this the right place to de-duplicate kmers?
    //Could easily do this as part of sorting in PathGraphBuilder.

    kmers.distinct.iterator.map(s => {
      val n = new KmerNode(s, 1)

      val suf = DNAHelpers.kmerSuffix(s, k)
      out.search(suf) match {
        //Pseudo-partition 1.
        //Setting the boundary flag blocks premature path finding.
        case Found(i) => n.boundaryPartition = Some(1)
        case _        =>
      }

      val pre = DNAHelpers.kmerPrefix(s, k)
      in.search(pre) match {
        case Found(i) => n.boundaryPartition = Some(1)
        case _        =>
      }

      n
    })
  }

  def kmerGraph(boundary: Iterable[String]) = {
    val builder = new PathGraphBuilder(k)
    builder.fromNodes(nodesForGraph(boundary).toArray)
  }

  /**
   * Find unitigs that we know to be contained in this bucket.
   * This includes unitigs that touch the boundary.
   */
  def containedUnitigs(boundary: Iterable[String]) = {
    new PathFinder(k).findSequences(kmerGraph(boundary))
  }


  /**
   * Remove sequences from the core of this bucket.
   */
  def removeSequences(ss: Iterable[NTSeq]): BoundaryBucket = {
    val filtered = removeKmers(ss, kmers, k)
    copy(core = filtered.toArray)
  }

  /**
   * Remove unitigs that are entirely contained in this bucket and do not
   * touch the boundary.
   * Returns the unitigs as well as an updated copy of the bucket with remaining
   * sequences.
   */
  def seizeUnitigs(boundary: Iterable[String]) = {
//    println(s"Core: ${core.toList}")
//    println(s"Boundary: ${boundary.toList}")

//    println("Taking unitigs from graph:")
//    kmerGraph(boundary).printBare()

    val unit = containedUnitigs(boundary)
    val (atBound, noBound) = unit.partition(_.touchesBoundary)

    val updated = removeSequences(noBound.map(_.seq))
    (noBound, updated)
  }

  /**
   * Split the core sequences into groups that are not connected
   * by any k-1 overlaps.
   */
  def splitSequences: List[List[String]] = {
    BoundaryBucket.splitSequences(core, k)
  }

  /**
   * For testing since arrays do not have deep equals
   */
  def structure = (id, core.toSet, k)

  override def toString = {
    s"BoundaryBucket($id\t${core.mkString(",")})"
  }

}