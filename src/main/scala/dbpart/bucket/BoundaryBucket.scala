package dbpart.bucket

import dbpart._
import dbpart.graph.{KmerNode, PathFinder, PathGraphBuilder}
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers

import scala.annotation.tailrec
import scala.collection.mutable.{Set => MSet}
import scala.collection.{Searching, Set => CSet}
import scala.reflect.ClassTag

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
    new OverlapFinder(prefixesAndSuffixes(data.iterator, k).toArray,
      purePrefixes(data.iterator, k).toArray,
      pureSuffixes(data.iterator, k).toArray,
      k)

  /**
   * A class that simplifies repeated checking for overlaps against some collection of k-mers.
   */
  case class OverlapFinder(val preAndSuf: Array[String], val pre: Array[String], val suf: Array[String],
                           k: Int) {
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

  /**
   * Split sequences into maximal groups that are not connected
   * by any k-1 overlaps.
   * The flag indicating whether a sequence is boundary or core (true iff boundary)
   * is preserved.
   */
  def splitSequences(ss: Seq[(String, Boolean)], k: Int): List[List[(String, Boolean)]] = {
    Util.partitionByKeys(ss.toList, (s: (String, Boolean)) => Read.kmers(s._1, k - 1).toList)
  }

  /**
   * Unify pairs of ID and sequences where the ID sequences have
   * intersection.
   */
  def unifyParts[T : ClassTag](parts: List[(List[Long], List[T])]) = {
    val unified = Util.partitionByKeys(parts, (x: (List[Long], List[T])) => x._1)
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

  /**
   * Remove unitigs from the core (whose boundary is known)
   * and split the bucket into parts without overlap.
   */
  def seizeUnitigsAndSplit(bucket: BoundaryBucket): (List[Contig], List[BoundaryBucket]) = {
    val (unitigs, updated) = bucket.seizeUnitigs

    //Split the core into parts that have no mutual overlap
    val split = updated.splitSequences

    val newBuckets = split.flatMap(s => {
      val (bound, core) = s.toArray.partition(_._2)
      if (!bound.isEmpty) {
        Some(BoundaryBucket(bucket.id, core.map(_._1), bound.map(_._1), bucket.k))
      } else {
        //All output at this stage
        None
      }
    })
    (unitigs, newBuckets)
  }
}

case class BoundaryBucketStats(coreSequences: Int, coreKmers: Int,
  boundarySequences: Int, boundaryKmers: Int)

/**
 * A bucket of sequences being merged together.
 * @param id
 * @param core Sequences whose neighborhood is fully known and inside this bucket.
 * @param boundary Sequences that may connect with other buckets.
 * @param k
 */
case class BoundaryBucket(id: Long, core: Array[String], boundary: Array[String], k: Int) {
  import BoundaryBucket._

  //TODO remove k

  def coreAndBoundary = core.iterator ++ boundary.iterator

  def kmers = coreKmers ++ boundaryKmers
  def coreKmers = core.flatMap(Read.kmers(_, k))
  def boundaryKmers = boundary.flatMap(Read.kmers(_, k))

  def coreStats = BoundaryBucketStats(core.size, core.map(Read.numKmers(_, k)).sum,
    boundary.size, boundary.map(Read.numKmers(_, k)).sum)

  def pureSuffixSet = pureSuffixes(boundary.iterator, k).toArray

  def purePrefixSet = purePrefixes(boundary.iterator, k).toArray

  def prefixAndSuffixSet = prefixesAndSuffixes(boundary.iterator, k).toArray

  /**
   * Constructs an overlap finder that can test this bucket's overlaps (through boundary)
   * with other buckets
   */
  def overlapFinder = new OverlapFinder(prefixAndSuffixSet, purePrefixSet,
    pureSuffixSet, k)

  def shiftBoundary(intersecting: Iterable[String]) = {
    val finder = new OverlapFinder(intersecting.toArray, Array(), Array(), k)
    val (stayInBound, boundToCore) = boundary.partition(b => finder.check(b))
    BoundaryBucket(id, core ++ boundToCore, stayInBound, k)
  }

  import Searching._

  def nodesForGraph: Iterator[KmerNode] = {
    //Possible optimization: try to do this with boundary as a simple iterator instead

    val in = sharedOverlapsThroughPrior(boundary, core, k).toArray.sorted
    val out = sharedOverlapsThroughPost(core, boundary, k).toArray.sorted

    //TODO is this the right place to de-duplicate kmers?
    //Could easily do this as part of sorting in PathGraphBuilder.

      coreKmers.distinct.iterator.map(s => {
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

  def kmerGraph = {
    val builder = new PathGraphBuilder(k)
    builder.fromNodes(nodesForGraph.toArray)
  }

  /**
   * Find unitigs that we know to be contained in this bucket.
   * This includes unitigs that touch the boundary.
   */
  def containedUnitigs =
    new PathFinder(k).findSequences(kmerGraph)

  /**
   * Remove sequences from the core of this bucket.
   */
  def removeSequences(ss: Iterable[NTSeq]): BoundaryBucket = {
    copy(core = removeKmers(ss, coreKmers, k).toArray)
  }

  /**
   * Remove unitigs that are entirely contained in this bucket and do not
   * touch the boundary.
   * Returns the unitigs as well as an updated copy of the bucket with remaining
   * sequences.
   */
  def seizeUnitigs = {
//    println(s"Core: ${core.toList}")
//    println(s"Boundary: ${boundary.toList}")

//    println("Taking unitigs from graph:")
//    kmerGraph.printBare()

    val unit = containedUnitigs
    val (atBound, noBound) = unit.partition(_.touchesBoundary)

    val updated = removeSequences(noBound.map(_.seq))
    (noBound, updated)
  }

  /**
   * Split the core sequences into groups that are not connected
   * by any k-1 overlaps.
   */
  def splitSequences: List[List[(String, Boolean)]] = {
    val withBoundaryFlag = core.map(x => (x, false)) ++
      boundary.map(x => (x, true))

    BoundaryBucket.splitSequences(withBoundaryFlag, k)
  }

  override def toString = s"BoundaryBucket($id\t${core.mkString(",")})\t${boundary.mkString(",")})"
}