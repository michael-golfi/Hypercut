package dbpart.bucket

import dbpart._
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers
import dbpart.graph.PathGraphBuilder
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder
import scala.collection.immutable.SortedSet

object BoundaryBucket {
  def apply(id: Long, components: List[(Array[String], Boolean)], k: Int): BoundaryBucket = {
    val (boundary, core) = components.partition(_._2)

    BoundaryBucket(id, core.flatMap(_._1).toArray,
      boundary.flatMap(_._1).toArray, k)
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
  def sharedOverlapsThroughPrior(prior: Seq[String], post: Seq[String], k: Int): Iterator[String] = {
    val preKmers = potentialOut(prior.iterator, k).toSet
    val postKmers = potentialIn(post.iterator, k)
    postKmers.filter(preKmers.contains)
  }

  /**
   * As above, but post gets cached as a set.
   */
  def sharedOverlapsThroughPost(prior: Seq[String], post: Seq[String], k: Int): Iterator[String] = {
    val preKmers = potentialOut(prior.iterator, k)
    val postKmers = potentialIn(post.iterator, k).toSet
    preKmers.filter(postKmers.contains)
  }

  /**
   * Remove unitigs from the core (whose boundary is known)
   * and merge it with its boundary buckets when there is still a shared (k-1)-mer
   * after unitig removal.
   * The returned buckets will share the same IDs as the input buckets, but some may have been
   * merged into the core.
   * The contents of buckets on the boundary, if not merged into the core, will be unchanged.
   */
  def seizeUnitigsAndMerge(core: BoundaryBucket,
                           boundary: List[BoundaryBucket]):
                           (List[Contig], List[BoundaryBucket], Array[(Long, Long)], Array[(Long, Long)]) = {
    val (noBound, updated) = core.seizeUnitigs
    val suffix = updated.potentialOutSet
    val prefix = updated.potentialInSet

    val (merge, noMerge) = boundary.partition(updated.overlapsWith(_, suffix, prefix))

    def removeEdges(ids: Seq[Long]) = ids.flatMap(i =>
      Seq((core.id, i), (i, core.id))).toArray

    val remove = removeEdges(noMerge.map(_.id))

    if (updated.core.isEmpty) {
      (noBound, noMerge, noMerge.map(x => (x.id -> x.id)).toArray, remove)
    } else {
      val newCore = BoundaryBucket(core.id, updated.core ++ merge.flatMap(_.core), Array(), core.k)
      val relabelIds = Array(core.id -> core.id) ++ merge.map(_.id -> core.id) ++ noMerge.map(x => (x.id -> x.id))
      (noBound, newCore :: noMerge, relabelIds, remove)
    }
  }
}

case class BoundaryBucket(id: Long, core: Array[String], boundary: Array[String],
  k: Int) {
  import BoundaryBucket._

  def kmers = core.flatMap(Read.kmers(_, k))

  def coreStats = BucketStats(core.size, 0, kmers.size)
  def boundaryStats = BucketStats(boundary.size, 0, 0)

  def overlapsWith(other: BoundaryBucket, suffix: Set[String], prefix: Set[String]) = {
    !sharedOverlapsTo(other.core.iterator, suffix).isEmpty ||
      !sharedOverlapsFrom(other.core.iterator, prefix).isEmpty
  }

  def sharedOverlapsFrom(prior: Iterator[String], inSet: Set[String]): Iterator[String] =
    potentialOut(prior, k).filter(inSet.contains(_))

  def sharedOverlapsTo(post: Iterator[String], outSet: Set[String]): Iterator[String] =
    potentialIn(post, k).filter(outSet.contains(_))

  /**
   * Openings forward of length k-1.
   * These are the k-1-mers that may be joined with outgoing sequences.
   */
  def outSet: Set[String] =
    sharedOverlapsThroughPrior(core, boundary, k).toSet

  /**
   * Openings backward of length k-1.
   * These are the k-1-mers that may be joined with incoming sequences.
   */
  def inSet: Set[String] =
    sharedOverlapsThroughPost(boundary, core, k).toSet

  def potentialOutSet = potentialOut(core.iterator, k).toSet

  def potentialInSet: Set[String] = potentialIn(core.iterator, k).toSet

  def isBoundary(seq: NTSeq, outSet: Set[String], inSet: Set[String]) =
    outSet.contains(DNAHelpers.kmerSuffix(seq, k)) ||
      inSet.contains(DNAHelpers.kmerPrefix(seq, k))

  def nodesForGraph = {
    val in = inSet
    val out = outSet


    kmers.iterator.map(s => {
      val n = new KmerNode(s, 1)
      if (isBoundary(s, out, in)) {
        //Pseudo-partition 1.
        //Setting the boundary flag blocks premature path finding.
        n.boundaryPartition = Some(1)
      }
      n
    })
  }

  def kmerGraph = {
    val builder = new PathGraphBuilder(k)
    builder.fromNodes(nodesForGraph.toList)
  }

  /**
   * Find unitigs that we know to be contained in this bucket.
   * This includes unitigs that touch the boundary.
   */
  def containedUnitigs = {
    new PathFinder(k).findSequences(kmerGraph)
  }

  /**
   * Remove sequences from the core of this bucket.
   */
  def removeSequences(ss: Iterable[NTSeq]): BoundaryBucket = {
    val removeKmers = ss.iterator.flatMap(Read.kmers(_, k)).toSet
    val filtered = Read.flattenKmers(kmers.filter(s => !removeKmers.contains(s)).
      toList, k, Nil)

    BoundaryBucket(id, filtered.toArray, boundary, k)
  }

  /**
   * Remove unitigs that are entirely contained in this bucket and do not
   * touch the boundary.
   * Returns the unitigs as well as an updated copy of the bucket with remaining
   * sequences.
   */
  def seizeUnitigs = {
    val unit = containedUnitigs
    val (atBound, noBound) = unit.partition(_.touchesBoundary)
    val updated = removeSequences(noBound.map(_.seq))
    (noBound, updated)
  }

}