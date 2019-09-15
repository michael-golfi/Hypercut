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

  def prefixes(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      //Number of (k-1)-mers
      val num = s.length() - (k - 2)
      Read.kmers(s, k - 1).take(num - 1)
    })

  def suffixes(ss: Iterator[String], k: Int) =
    ss.flatMap(Read.kmers(_, k - 1).drop(1))

  /**
   * Function for computing shared k-1 length overlaps between sequences of prior
   * and post k-mers. Intended for computing inOpenings and outOpenings.
   * Prior gets cached as a set.
   */
  def sharedOverlapsThroughPrior(prior: Iterator[String], post: Iterator[String], k: Int): Iterator[String] = {
    val preKmers = suffixes(prior, k).to[MSet]
    val postKmers = prefixes(post, k)
    postKmers.filter(preKmers.contains)
  }

  /**
   * As above, but post gets cached as a set.
   */
  def sharedOverlapsThroughPost(prior: Iterator[String], post: Iterator[String], k: Int): Iterator[String] = {
    val preKmers = suffixes(prior, k)
    val postKmers = prefixes(post, k).to[MSet]
    preKmers.filter(postKmers.contains)
  }

  def overlapsWith(other: Iterable[String], suffix: CSet[String], prefix: CSet[String], k: Int) = {
    !sharedOverlapsTo(other.iterator, suffix, k).isEmpty ||
      !sharedOverlapsFrom(other.iterator, prefix, k).isEmpty
  }

  def overlapsWith(other: BoundaryBucket, suffix: CSet[String], prefix: CSet[String], k: Int): Boolean =
    overlapsWith(other.core, suffix, prefix, k)

  def sharedOverlapsFrom(prior: Iterator[String], inSet: CSet[String], k: Int): Iterator[String] =
    suffixes(prior, k).filter(inSet.contains(_))

  def sharedOverlapsTo(post: Iterator[String], outSet: CSet[String], k: Int): Iterator[String] =
    prefixes(post, k).filter(outSet.contains(_))

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
   * From an iterator of k-mers, remove any k-mers contained in
   * the set of (potentially long) existing sequences, and join the iterator back into strings as much as possible.
   */
  def withoutDuplicates(existingSeq: Iterator[NTSeq], incomingKmers: List[NTSeq], k: Int): List[String] = {
    val removeKmers = existingSeq.flatMap(Read.kmers(_, k)).to[MSet]
    Read.flattenKmers(incomingKmers.filter(s => !removeKmers.contains(s)), k, Nil)
  }

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

    //Pair each split part with the boundary IDs that it intersects with
    val splitWithBoundary = split.map(s => {
      //Note: might save memory by sharing strings between these two sets
      val potentialSuffixes = suffixes(s.iterator, core.k).to[MSet]
      val potentialPrefixes = prefixes(s.iterator, core.k).to[MSet]

      val merge = boundary.filter(
        overlapsWith(_, potentialSuffixes, potentialPrefixes, core.k))
      (merge.map(_.id), s)
    }).filter(_._1.nonEmpty)

    //Split parts that had no intersection with the boundary will be output as
    //unitigs at this stage, so OK to drop them

    val mergedParts = unifyParts(splitWithBoundary)
//    val nonOverlapBoundary = (boundary.map(_.id).to[MSet] -- (mergedParts.flatMap(_._2))).toSeq

    def removableEdges(ids: Seq[Long]) = ids.flatMap(i =>
      Seq((core.id, i), (i, core.id))).toArray

    //Boundary buckets will have edges to/from the old core removed
    val remove = removableEdges(boundary.map(_.id))

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

  def suffixSet: CSet[String] = suffixes(core.iterator, k).to[MSet]

  def prefixSet: CSet[String] = prefixes(core.iterator, k).to[MSet]

  import Searching._

  def nodesForGraph(boundary: Iterable[String]) = {
    //Possible optimization: try to do this with boundary as a simple iterator instead
    val in = sharedOverlapsThroughPost(boundary.iterator, core.iterator, k).toArray.sorted
    val out = sharedOverlapsThroughPrior(core.iterator, boundary.iterator, k).toArray.sorted

    //TODO is this the right place to de-duplicate kmers?
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
    builder.fromNodes(nodesForGraph(boundary).toList)
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
    val filtered = withoutDuplicates(ss.iterator, kmers.toList, k)
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

  override def toString = {
    s"BoundaryBucket($id\t${core.mkString(",")}"
  }

}