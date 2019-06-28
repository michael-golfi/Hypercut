package dbpart.bucket

import dbpart.shortread.Read
import dbpart.graph.PathGraphBuilder
import miniasm.genome.util.DNAHelpers
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder
import dbpart._

object PathMergingBucket {
  /**
   * Function for computing shared k-1 length overlaps between sequences of prior
   * and post k-mers. Intended for computing inOpenings and outOpenings.
   */
  def sharedOverlaps(prior: Seq[String], post: Seq[String], k: Int): Array[String] = {
     val preKmers = prior.iterator.flatMap(Read.kmers(_, k - 1).drop(1)).toSet
     val postKmers = post.toSeq.flatMap(Read.kmers(_, k - 1).toSeq.dropRight(1))
     postKmers.filter(x => preKmers.contains(x)).distinct.toArray
  }
}

/**
 * Bucket for progressively merging paths into longer ones.
 * Paths either pass through this bucket, begin in it, or end in it.
 * At every step we track remaining openings: (k-1)-mers that are potential incoming
 * or outgoing merges.
 *
 * As with CountingSeqBucket, data is stored as sequences that are formed from
 * overlapping k-mers.
 */
case class PathMergingBucket(val sequences: Array[String],
  val k: Int,

  /**
   * Openings forward of length k-1.
   * These are the k-1-mers that may be joined with outgoing sequences.
   */
  val outOpenings: Array[String] = Array(),

  /**
   * Openings backward of length k-1.
   * These are the k-1-mers that may be joined with incoming sequences.
   */
  val inOpenings: Array[String] = Array()) {

  def kmers = sequences.toSeq.flatMap(Read.kmers(_, k))

  def removeSequences(ss: Iterable[NTSeq]): PathMergingBucket = {
    val removeKmers = ss.iterator.flatMap(Read.kmers(_, k)).toSet
    val filtered = Read.flattenKmers(kmers.filter(s => !removeKmers.contains(s)).
      toList, k, Nil)

    val remKmers = filtered.flatMap(Read.kmers(_, k))

    val filteredSuffix = remKmers.map(DNAHelpers.kmerSuffix(_, k)).toSet
    val filteredPrefix = remKmers.map(DNAHelpers.kmerPrefix(_, k)).toSet
    val filterOut = outOpenings.filter(filteredSuffix.contains)
    val filterIn = inOpenings.filter(filteredPrefix.contains)
    PathMergingBucket(filtered.toArray, k, filterOut, filterIn)
  }

  @transient
  lazy val outSet = outOpenings.toSet
  @transient
  lazy val inSet = inOpenings.toSet

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
   * Find unitigs that we know to be fully contained in this bucket.
   */
  def containedUnitigs = {
    new PathFinder(k).findSequences(kmerGraph)
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
   * Join incoming paths from a different bucket,
   * remove any finished unitigs from this bucket, returning them and a new merged
   * bucket with unfinished data from the two input buckets.
   */
  def mergeIn(in: PathMergingBucket): (Iterable[String], PathMergingBucket) = {
    ???
  }

  def focusIn(openings: Array[String]): PathMergingBucket = ???

  def focusOut(openings: Array[String]): PathMergingBucket = ???
}