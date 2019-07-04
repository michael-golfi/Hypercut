package dbpart.bucket

import dbpart._
import dbpart.shortread.Read
import miniasm.genome.util.DNAHelpers
import dbpart.graph.PathGraphBuilder
import dbpart.graph.KmerNode
import dbpart.graph.PathFinder

object BoundaryBucket {
  def apply(boundarySequences: Array[String], k: Int): BoundaryBucket =
    BoundaryBucket(Array(), boundarySequences, k)

  def apply(components: List[(Array[String], Boolean)], k: Int): BoundaryBucket = {
    val (boundary, core) = components.partition(_._2)

    BoundaryBucket(core.flatMap(_._1).toArray,
      boundary.flatMap(_._1).toArray, k)
  }
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

case class BoundaryBucket(core: Array[String], boundary: Array[String],
  k: Int) {
  import BoundaryBucket._

  def kmers = core.toSeq.flatMap(Read.kmers(_, k))

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

    BoundaryBucket(filtered.toArray, boundary, k)
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