package dbpart.graph

import scala.annotation.tailrec

import dbpart._
import dbpart.hash.MarkerSpace
import friedrich.graph.Graph
import miniasm.genome.util.DNAHelpers

/**
 * Simple PathGraphBuilder that constructs a graph from a predefined set of kmers.
 */
class PathGraphBuilder(k: Int) {

  def build(kmers: List[String]) = {        
    //Currently setting all abundances to 1
    val nodes = kmers.map(s => new KmerNode(s, 1))
    val result = new DoubleArrayListGraph[KmerNode](nodes.toArray)

    val byStart = nodes.sortBy(_.seq)
    val byEnd = nodes.sortBy(n => n.end)

    val gotEdges = findEdges(byStart, byEnd, false,
          (f, t) => result.addEdge(f,t))
    result
  }

  /**
   * Find edges by traversing two k-mer lists, one sorted by the first
   * k-1 bases and the other sorted by the final k-1.
   * Applies the given function to each edge that was found.
   */
  @tailrec
  final def findEdges(byBeginning: List[KmerNode], byEnd: List[KmerNode],
                gotEdges: Boolean, f: (KmerNode, KmerNode) => Unit): Boolean = {
    byBeginning match {
      case b :: bs =>
        byEnd match {
          case e :: es =>
            val overlap = e.end
            val cmp = overlap.compareTo(b.begin)
            if (cmp < 0) {
              findEdges(byBeginning, es, gotEdges, f)
            } else if (cmp > 0) {
              findEdges(bs, byEnd, gotEdges, f)
            } else {
              val ns = byEnd.takeWhile(_.end == overlap)
              for (n <- ns) { f(n, b) }
              //All possible edges for b have been constructed
              findEdges(bs, byEnd, true, f)
            }
          case _ => gotEdges
        }
      case _ => gotEdges
    }
  }
}

/**
 * Based on a partitioned "macro graph" of buckets,
 * builds a path graph that contains edges between k-mers in a partition.
 */
final class MacroPathGraphBuilder(loader: BucketLoader, k: Int,
    nodes: Iterable[MacroNode],
    macroGraph: Graph[MacroNode])(implicit space: MarkerSpace) extends PathGraphBuilder(k) {

  println(s"Construct path graph from ${nodes.size} macro nodes (${nodes.filter(_.isBoundary).size} boundary)")
  val result = addNodes(nodes)

  def sequenceToKmerNodes(macroNode: MacroNode, seqs: Iterator[String], abunds: Iterator[Abundance]) =
    (seqs zip abunds).toList.map(s => new KmerNode(s._1, s._2))

  def loadKmers(part: Iterable[MacroNode]) = {
    val partMap = Map() ++ part.map(x => x.uncompact -> (x.id, x))
    val bulkData = loader.getBuckets(part.map(_.uncompact))

    Map() ++ (for {
      (key, bucket) <- bulkData
      macroNode = partMap(key)._2
      id = partMap(key)._1
      kmers = bucket.kmersBySequenceWithAbundance.toList.flatMap(x =>
        sequenceToKmerNodes(macroNode, x._1, x._2.iterator))
    } yield (id, kmers))
  }

  /**
   * Add marker set buckets to the path graph, constructing all edges between k-mers
   * in the buckets of this partition.
   */
  def addNodes(part: Iterable[MacroNode]) = {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")

    val partSet = part.map(_.id).toSet

    //Load buckets potentially outside the partition, to allow us to handle the boundary
    //correctly
    val partAndBoundary = (part.toSeq ++
        part.flatMap(n => macroGraph.edges(n))).distinct
    val kmers = loadKmers(partAndBoundary)

    val result = new DoubleArrayListGraph[KmerNode](
        kmers.filter(n => partSet.contains(n._1)).values.toList.flatten.toArray)

    val byStart = kmers.map(x => (x._1 -> x._2.sortBy(_.seq)))

    //Tracking bucket pairs with actual k-mer edges being constructed
    var foundEdges = 0

    for {
      fromBucket <- part
      byEnd = kmers(fromBucket.id).sortBy(_.end)
      toBuckets = macroGraph.edgesFrom(fromBucket)
      toBucket <- fromBucket :: toBuckets //Always add an edge back to the same bucket
    } {
      if (partSet.contains(toBucket.id)) {
        val gotEdges = findEdges(byStart(toBucket.id), byEnd, false,
          (f, t) => result.addEdge(f,t))
        if (gotEdges) {
          foundEdges += 1
        }
      } else {
        //Edge into other partition - set the boundary flag
        findEdges(byStart(toBucket.id), byEnd, false,
          (f, t) => f.boundaryPartition = Some(toBucket.partitionId))
      }
    }

    for {
      toBucket <- part
      fromBuckets = macroGraph.edgesTo(toBucket)
      fromBucket <- fromBuckets
      if ! partSet.contains(fromBucket.id)
      byEnd = kmers(fromBucket.id).sortBy(_.end)
    } {
      //Edge from other partition - set boundary flag
      findEdges(byStart(toBucket.id), byEnd, false,
        (f, t) => t.boundaryPartition = Some(fromBucket.partitionId))
    }

    println(s"$foundEdges bucket pairs have k-mer edges")
    result
  }
}

/**
 * Bubble collapsing algorithm, lifted more or less as it is
 * from Friedrich's miniasm
 */
final class PathGraphAnalyzer(g: Graph[KmerNode], k: Int) {
  type N = KmerNode

  /**
   * A list of nodes that have multiple forward edges.
   */
  def findForwardJunctions(): Iterator[N] =
    g.nodes.filter(g.edgesFrom(_).size > 1)

  /**
   * *
   * A list of nodes that have multiple backward edges.
   */
  def findBackwardJunctions(): Iterator[N] =
    g.nodes.filter(g.edgesTo(_).size > 1)

  /**
   * *
   * Set all nodes in this path to ignore
   */
  def setNoise(path: Iterable[N]): Unit = {
//    println("Set noise: " + path.mkString(" "))
    path.foreach { _.noise = true }
  }

  /**
   * *
   * Un-ignore all nodes in this path
   */
  def unsetNoise(path: Iterable[N]): Unit = {
//    println("Unset noise: " + path.mkString(" "))
    path.foreach { _.noise = false }
  }

  /**
   * *
   * Set all paths in list to ignore
   */
  def setAllNoise(paths: Iterable[Iterable[N]]): Unit =
    paths.foreach { setNoise(_) }

  /**
   * *
   * Compute the average coverage of this path
   */
  def computePathAbundance(path: Iterable[N]): Double =
    path.map(p => p.abundance).sum.toDouble / path.size

  final def transform(list: List[List[N]]): List[List[N]] = transform(Nil, list)

  @tailrec
  final def transform(acc: List[List[N]], list: List[List[N]]): List[List[N]] =
    list match {
      case (m :: ms) :: ls => {
        val ef = g.edgesFrom(m).filter(e => ! e.noise && ! ms.contains(e))
        if (ef.size > 0) {
          //ef.toList.map(_ :: m :: ms) ++ transform(ls)
          transform(ef.toList.map(_ :: m :: ms) ::: acc, ls)
        } else {
          //          (m :: ms) :: transform(ls)
          transform((m :: ms) :: acc, ls)
        }
      }
      case _ => acc
    }

  /**
   * Note: This bubble collapsing algorithm is currently too memory intensive,
   * doesn't scale well to large jobs
   */
  def findBubbles() = {
    var count = 1
    var gcount = 0
    var reduced = 0
    var found = false
    var paths = List[List[N]]()

    val junctions = findForwardJunctions().toList
    println("total of " + junctions.size + " branches to inspect")
    for {
      split <- junctions.par
      if ! split.noise
    } {
      //println("branch node: " + split)
      paths = g.edgesFrom(split).map(_ :: Nil).toList
      while ((!found) && (count < k * 3)) {
        paths = transform(paths)
        if (haveIntersection(paths)) {
          paths = paths.map(_.reverse)
          setAllNoise(paths)
          unsetNoise(paths.sortWith((p, q) => computePathAbundance(p) > computePathAbundance(q)).head)
          found = true
          reduced += 1
        }
        count += 1
      }
      found = false
      count = 1
      gcount += 1
    }
    println(s"Reduced $reduced bubbles")

    def haveIntersection(paths: List[List[N]]) = {
      paths match {
        case p :: ps =>
          !paths.tail.foldLeft(p.toSet)((a, b) => b.filter(a.contains(_)).toSet).isEmpty
        case _ => false
      }
    }

  }
}