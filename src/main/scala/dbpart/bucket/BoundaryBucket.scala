package dbpart.bucket

import dbpart._
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers
import dbpart.graph.PathGraphBuilder
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder

object BoundaryBucket {
  def apply(id: Long, components: List[(Array[String], Boolean)], k: Int): BoundaryBucket = {
    val (boundary, core) = components.partition(_._2)

    BoundaryBucket(id, core.flatMap(_._1).toArray,
      boundary.flatMap(_._1).toArray, k)
  }

  def potentialIn(ss: Seq[String], k: Int) =
    ss.flatMap(Read.kmers(_, k - 1).toSeq.dropRight(1))

  def potentialOut(ss: Iterator[String], k: Int) =
    ss.flatMap(Read.kmers(_, k - 1).drop(1))

  /**
   * Function for computing shared k-1 length overlaps between sequences of prior
   * and post k-mers. Intended for computing inOpenings and outOpenings.
   */
  def sharedOverlaps(prior: Seq[String], post: Seq[String], k: Int): Array[String] = {
    val preKmers = prior.iterator.flatMap(Read.kmers(_, k - 1).drop(1)).toSet
    val postKmers = post.toSeq.flatMap(Read.kmers(_, k - 1).toSeq.dropRight(1))
    postKmers.filter(x => preKmers.contains(x)).distinct.toArray
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
    val (merge, noMerge) = boundary.partition(updated.overlapsWith)

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

  def overlapsWith(other: BoundaryBucket) = {
    //Note: could make this direction-aware
    !sharedOverlapsTo(other.core).isEmpty ||
      !sharedOverlapsFrom(other.core).isEmpty
  }

  /**
   * Openings forward of length k-1.
   * These are the k-1-mers that may be joined with outgoing sequences.
   */
  @transient
  lazy val outSet: Set[String] = {
    sharedOverlaps(core, boundary, k).toSet
  }

  /**
   * Openings backward of length k-1.
   * These are the k-1-mers that may be joined with incoming sequences.
   */
  @transient
  lazy val inSet: Set[String] = {
    sharedOverlaps(boundary, core, k).toSet
  }

  @transient
  lazy val potentialOutSet = potentialOut(core.iterator, k).toSet

  @transient
  lazy val potentialInSet: Set[String] = potentialIn(core, k).toSet

  def sharedOverlapsFrom(prior: Seq[String]): Array[String] = {
    potentialOut(prior.iterator, k).
      filter(potentialInSet.contains(_)).toArray
  }

  def sharedOverlapsTo(post: Seq[String]): Array[String] = {
    potentialIn(post, k).
      filter(potentialOutSet.contains(_)).toArray
  }

  def isBoundary(seq: NTSeq) =
    outSet.contains(DNAHelpers.kmerSuffix(seq, k)) ||
      inSet.contains(DNAHelpers.kmerPrefix(seq, k))

  def kmerGraph = {
    val builder = new PathGraphBuilder(k)

    val nodes = kmers.iterator.map(s => {
      val n = new KmerNode(s, 1)
      if (isBoundary(s)) {
        //Pseudo-partition 1.
        //Setting the boundary flag blocks premature path finding.
        n.boundaryPartition = Some(1)
      }
      n
    })

    builder.fromNodes(nodes.toList)
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