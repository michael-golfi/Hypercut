package dbpart.bucket

import dbpart._
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers
import dbpart.graph.PathGraphBuilder
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder
import scala.collection.immutable.SortedSet
import scala.collection.Searching
import scala.collection.mutable.{Map => MMap, Set => MSet}

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

  def overlapsWith(other: BoundaryBucket, suffix: Set[String], prefix: Set[String], k: Int) = {
    !sharedOverlapsTo(other.core.iterator, suffix, k).isEmpty ||
      !sharedOverlapsFrom(other.core.iterator, prefix, k).isEmpty
  }

  def sharedOverlapsFrom(prior: Iterator[String], inSet: Set[String], k: Int): Iterator[String] =
    potentialOut(prior, k).filter(inSet.contains(_))

  def sharedOverlapsTo(post: Iterator[String], outSet: Set[String], k: Int): Iterator[String] =
    potentialIn(post, k).filter(outSet.contains(_))

   /**
   * Split sequences into groups that are not connected
   * by any k-1 overlaps.
   */
  def splitSequences(ss: Seq[String], k: Int): List[List[String]] = {
    val lookup = MMap[String, List[String]]()

    for (s <- ss) {
      Read.kmers(s, k-1).find(lookup.contains) match {
        case Some(key) =>
          lookup(key) = s :: lookup(key)
        case None =>
          lookup += (Read.kmers(s, k-1).next -> List(s))
      }
    }
    lookup.values.toList
  }

  /**
   * Unify pairs of ID and sequences where the ID sequences have
   * intersection.
   */
  def unifyParts(parts: List[(List[Long], List[String])]) = {
    //unmerged to merged
    var idLookup = MMap[Long, Long]()
    //merged to data
    var dataLookup = MMap[Long, Array[String]]()

    for (p <- parts) {
      //The part may exist in multiple partitions
      val hadKeys = p._1.filter(idLookup.contains)
      if (hadKeys.nonEmpty) {
        val ids = hadKeys ++ p._1
        val data = hadKeys.map(idLookup).distinct.map(dataLookup).flatten ++ p._2
        val min = ids.min
        val oldKeysRewrite = Map() ++ hadKeys.map(k => (idLookup(k) -> min))

        for (i <- ids) {
          idLookup += (i -> min)
        }
        idLookup = idLookup.map(x => (x._1 -> oldKeysRewrite.getOrElse(x._2, x._2)))
        val remainingKeys = idLookup.values.toSet
        dataLookup = dataLookup.filter(x => remainingKeys.contains(x._1))
        dataLookup(min) = data.toArray

      } else {
        val min = p._1.min
        for (i <- p._1) {
          idLookup += i -> min
        }
        dataLookup(min) = p._2.toArray
      }
    }
    val rev = idLookup.toSeq.groupBy(_._2)
    //Triplets of:
    //ID (pre-existing), new sequences from core, IDs of boundary buckets to merge with this part
    dataLookup.map(x => (x._1, x._2, rev(x._1).map(_._1))).toList
  }

  /**
   * Remove unitigs from the core (whose boundary is known)
   * and merge it with its boundary buckets when there is still a shared (k-1)-mer
   * after unitig removal.
   * The returned buckets will share the same IDs as the input buckets, but some may have been
   * merged into the core.
   * The contents of buckets on the boundary, if not merged into the core, will be unchanged.
   *
   * Returns: Contigs ready for output, new buckets, relabelled IDs, edges to be removed.
   */
  def seizeUnitigsAndMerge(core: BoundaryBucket,
                           boundary: List[BoundaryBucket]):
                           (List[Contig], List[BoundaryBucket], Array[(Long, Long)], Array[(Long, Long)]) = {
    val (unitigs, updated) = core.seizeUnitigs

    //Split the core into parts that have no mutual overlap
    val split = updated.splitSequences

    //Pair each split part with the boundary IDs that it intersects with
    val splitWithBoundary = split.map(s => {
      val potentialSuffixes = potentialOut(s.iterator, core.k).toSet
      val potentialPrefixes = potentialIn(s.iterator, core.k).toSet

      val (merge, noMerge) = boundary.partition(
        overlapsWith(_, potentialSuffixes, potentialPrefixes, core.k))
      (merge.map(_.id), s)
    }).filter(_._1.nonEmpty)

    val mergedParts = unifyParts(splitWithBoundary.map(x => (x._1, x._2)))
    val noMerge = (boundary.map(_.id).toSet -- (mergedParts.flatMap(_._3))).toSeq
    val boundaryLookup = Map() ++ boundary.map(b => (b.id -> b))

    def removableEdges(ids: Seq[Long]) = ids.flatMap(i =>
      Seq((core.id, i), (i, core.id))).toArray

    //Boundary buckets will have edges to/from the old core removed
    val remove = removableEdges(boundary.map(_.id))

    if (splitWithBoundary.isEmpty) {
      (unitigs, boundary, boundary.map(x => (x.id -> x.id)).toArray, remove)
    } else {
      val mbs = mergedParts.map(m =>
        BoundaryBucket(m._1, m._2 ++ m._3.flatMap(b => boundaryLookup(b).core), Array(), core. k)
      )

      val relabelIds = mergedParts.flatMap(m => m._3.map(_ -> m._1)) ++ noMerge.map(x => (x -> x))
      (unitigs, mbs, relabelIds.toArray, remove)
    }
  }
}

case class BoundaryBucket(id: Long, core: Array[String], boundary: Array[String],
  k: Int) {
  import BoundaryBucket._

  def kmers = core.flatMap(Read.kmers(_, k))

  def coreStats = BucketStats(core.size, 0, kmers.size)
  def boundaryStats = BucketStats(boundary.size, 0, 0)

  def potentialOutSet = potentialOut(core.iterator, k).toSet

  def potentialInSet: Set[String] = potentialIn(core.iterator, k).toSet

  import Searching._

  def nodesForGraph = {
    val in = sharedOverlapsThroughPost(boundary, core, k).toArray.sorted
    val out = sharedOverlapsThroughPrior(core, boundary, k).toArray.sorted

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

  /**
   * Split the core sequences into groups that are not connected
   * by any k-1 overlaps.
   */
  def splitSequences: List[List[String]] = {
    BoundaryBucket.splitSequences(core, k)
  }

}