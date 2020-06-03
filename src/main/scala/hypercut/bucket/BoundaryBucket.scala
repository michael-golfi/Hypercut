package hypercut.bucket

import hypercut._
import hypercut.graph.{Contig, KmerNode, PathFinder, PathGraphBuilder}
import hypercut.shortread.Read
import miniasm.genome.util.DNAHelpers

import scala.collection.mutable.{Set => MSet}
import scala.collection.{Searching, Set => CSet}
import scala.reflect.ClassTag

object BoundaryBucket {
  import Util._

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

  final class Operations(bucket: BoundaryBucket, k: Int) {
    import Searching._

    def core = bucket.core
    def boundary = bucket.boundary
    def id = bucket.id

    def kmers = coreKmers ++ boundaryKmers
    def coreKmers = core.flatMap(Read.kmers(_, k))
    def boundaryKmers = boundary.flatMap(Read.kmers(_, k))
    def allSequences = core ++ boundary

    def meanSeqLength: Double = if (allSequences.isEmpty) 0 else
      allSequences.map(s => s.length - (k - 1)).sum.toDouble / allSequences.length

    def coreStats = BoundaryBucketStats(core.size, meanSeqLength,
      core.map(Read.numKmers(_, k)).sum,
      boundary.size, boundary.map(Read.numKmers(_, k)).sum)

    /**
     * Constructs an overlap finder that can test this bucket's overlaps (through boundary)
     * with other buckets
     */
    def overlapFinder = {
      val pureSuffixSet = pureSuffixes(boundary.iterator, k).toArray
      val purePrefixSet = purePrefixes(boundary.iterator, k).toArray
      val prefixAndSuffixSet = prefixesAndSuffixes(boundary.iterator, k).toArray

      new SetOverlapFinder(prefixAndSuffixSet, purePrefixSet,
        pureSuffixSet, k)
    }

    def shiftBoundary(intersecting: Iterable[String]) = {
      val finder = new SetOverlapFinder(intersecting.toArray, Array(), Array(), k)
      val (stayInBound, boundToCore) = boundary.partition(b => finder.check(b))
      BoundaryBucket(id, core ++ boundToCore, stayInBound)
    }


    def nodesForGraph: Iterator[KmerNode] = {
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
      bucket.copy(core = removeKmers(ss, coreKmers, k).toArray)
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

    /**
     * Remove unitigs from the core (whose boundary is known)
     * and split the bucket into parts without overlap.
     */
    def seizeUnitigsAndSplit: (List[Contig], List[BoundaryBucket]) = {
      val (unitigs, updated) = seizeUnitigs

      //Split the core into parts that have no mutual overlap
      val split = updated.ops(k).splitSequences

      val newBuckets = split.flatMap(s => {
        val (bound, core) = s.toArray.partition(_._2)
        if (!bound.isEmpty) {
          Some(BoundaryBucket(id, core.map(_._1), bound.map(_._1)))
        } else {
          //All output at this stage
          None
        }
      })
      (unitigs, newBuckets)
    }
  }
}

final case class BoundaryBucketStats(coreSequences: Int, seqLength: Double, coreKmers: Int,
  boundarySequences: Int, boundaryKmers: Int) {

  def numKmers: Long = coreKmers.toLong + boundaryKmers

}


/**
 * A bucket of sequences being merged together.
 * @param id
 * @param core Sequences whose neighborhood is fully known and inside this bucket.
 * @param boundary Sequences that may connect with other buckets.
 */
final case class BoundaryBucket(id: Long, core: Array[String], boundary: Array[String]) {

  def ops(k: Int) = new BoundaryBucket.Operations(this, k)
  def coreAndBoundary = core.iterator ++ boundary.iterator
  def numSequences = core.length + boundary.length

  override def toString = s"BoundaryBucket($id\t${core.mkString(",")})\t${boundary.mkString(",")})"
}
