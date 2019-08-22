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

  def potentialIn(ss: Iterator[String], k: Int) =
    ss.flatMap(s => {
      //Number of (k-1)-mers
      val num = s.length() - (k - 2)
      Read.kmers(s, k - 1).take(num - 1)
    })

  def potentialOut(ss: Iterator[String], k: Int) =
    ss.flatMap(Read.kmers(_, k - 1).drop(1))

  /**
   * Function for computing shared k-1 length overlaps between sequences of prior
   * and post k-mers. Intended for computing inOpenings and outOpenings.
   * Prior gets cached as a set.
   */
  def sharedOverlapsThroughPrior(prior: Iterator[String], post: Iterator[String], k: Int): Iterator[String] = {
    val preKmers = potentialOut(prior, k).to[MSet]
    val postKmers = potentialIn(post, k)
    postKmers.filter(preKmers.contains)
  }

  /**
   * As above, but post gets cached as a set.
   */
  def sharedOverlapsThroughPost(prior: Iterator[String], post: Iterator[String], k: Int): Iterator[String] = {
    val preKmers = potentialOut(prior, k)
    val postKmers = potentialIn(post, k).to[MSet]
    preKmers.filter(postKmers.contains)
  }

  def overlapsWith(other: BoundaryBucket, suffix: CSet[String], prefix: CSet[String], k: Int) = {
    !sharedOverlapsTo(other.core.iterator, suffix, k).isEmpty ||
      !sharedOverlapsFrom(other.core.iterator, prefix, k).isEmpty
  }

  def sharedOverlapsFrom(prior: Iterator[String], inSet: CSet[String], k: Int): Iterator[String] =
    potentialOut(prior, k).filter(inSet.contains(_))

  def sharedOverlapsTo(post: Iterator[String], outSet: CSet[String], k: Int): Iterator[String] =
    potentialIn(post, k).filter(outSet.contains(_))

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
      (ks.min, vs.toArray, ks)
    })
  }

  //Contigs ready for output, new buckets, relabelled IDs, edges to be removed (in terms of old IDs)
  type MergedBuckets = (List[Contig], List[BoundaryBucket], Array[(Long, Long)], Array[(Long, Long)])

  /**
   * Remove unitigs from the core (whose boundary is known)
   * and merge it with its boundary buckets when there is still a shared (k-1)-mer
   * after unitig removal.
   * The returned buckets will share the same IDs as the input buckets, but some may have been
   * merged into the core.
   * The contents of buckets on the boundary, if not merged into the core, will be unchanged.
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
        val potentialSuffixes = potentialOut(s.iterator, core.k).to[MSet]
        val potentialPrefixes = potentialIn(s.iterator, core.k).to[MSet]

        val merge = boundary.filter(
          overlapsWith(_, potentialSuffixes, potentialPrefixes, core.k))
        (merge.map(_.id), s)
      }).filter(_._1.nonEmpty)

      //Split parts that had no intersection with the boundary will be output as
      //unitigs at this stage, so OK to drop them

      val mergedParts = unifyParts(splitWithBoundary)
      val nonOverlapBoundary = (boundary.map(_.id).to[MSet] -- (mergedParts.flatMap(_._3))).toSeq
      val boundaryLookup = Map() ++ boundary.map(b => (b.id -> b))

      def removableEdges(ids: Seq[Long]) = ids.flatMap(i =>
        Seq((core.id, i), (i, core.id))).toArray

      //Boundary buckets will have edges to/from the old core removed
      val remove = removableEdges(boundary.map(_.id))

      val nonIntersectParts = identityMerge(nonOverlapBoundary.map(boundaryLookup))

      val intersectParts = mergedParts.map(m => {
        val fromCore = m._2
        val fromBoundaryOverlaps = m._3.flatMap(b => boundaryLookup(b).core)
        (BoundaryBucket(m._1, fromCore ++ fromBoundaryOverlaps, core.k, core.generation - 1, 1),
          m._3.map(_ -> m._1).toArray)
      })

    (unitigs, List() ++ nonIntersectParts._2 ++ intersectParts.map(_._1),
      Array() ++ nonIntersectParts._3 ++ intersectParts.flatMap(_._2),
      remove)
    }

  def simpleMerge(id: Long, k: Int, parts: Iterable[BoundaryBucket]): MergedBuckets = {
    (List(),
        List(BoundaryBucket(id, parts.iterator.flatMap(_.core).toArray, k,
          parts.map(_.generation).min, parts.map(_.nodeCount).sum)),
        parts.map(p => (p.id -> id)).toArray, Array())
  }

  /**
   * As above, but no merge is performed, and all IDs and edges are preserved.
   */
  def identityMerge(parts: Iterable[BoundaryBucket]): MergedBuckets = {
    (List(), parts.toList, parts.map(x => (x.id -> x.id)).toArray, Array())
  }
}

case class BoundaryBucket(id: Long, core: Array[String], k: Int, generation: Int = 0, nodeCount: Int = 1) {
  import BoundaryBucket._

  def kmers = core.flatMap(Read.kmers(_, k))

  def coreStats = BucketStats(core.size, 0, kmers.size, generation, nodeCount)

  def potentialOutSet: CSet[String] = potentialOut(core.iterator, k).to[MSet]

  def potentialInSet: CSet[String] = potentialIn(core.iterator, k).to[MSet]

  import Searching._

  def nodesForGraph(boundary: Iterable[String]) = {
    //Possible optimization: try to do this with boundary as a simple iterator instead
    val in = sharedOverlapsThroughPost(boundary.iterator, core.iterator, k).toArray.sorted
    val out = sharedOverlapsThroughPrior(core.iterator, boundary.iterator, k).toArray.sorted

    kmers.iterator.map(s => {
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
    val removeKmers = ss.iterator.flatMap(Read.kmers(_, k)).to[MSet]
    val filtered = Read.flattenKmers(kmers.filter(s => !removeKmers.contains(s)).
      toList, k, Nil)

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