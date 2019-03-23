package dbpart.graph

import dbpart._
import dbpart.ubucket.SeqBucketDB
import dbpart.MarkerSet
import friedrich.graph.Graph
import scala.annotation.tailrec
import friedrich.util.formats.GraphViz
import scala.collection.mutable.ArrayBuffer

/**
 * Builds a graph that contains edges between k-mers in a partition.
 */
final class PathGraphBuilder(pathdb: SeqBucketDB,
    nodes: Iterable[MacroNode],
    macroGraph: Graph[MacroNode])(implicit space: MarkerSpace) {

  val k: Int = pathdb.k
  var result: Graph[KmerNode] = _

  println(s"Construct path graph from ${nodes.size} macro nodes (${nodes.filter(_.isBoundary).size} boundary)")
  addNodes(nodes)

  def sequenceToKmerNodes(macroNode: MacroNode, seqs: Iterator[String], covs: Iterator[Int]) =
    (seqs zip covs).toList.map(s => new KmerNode(s._1, s._2, macroNode.isBoundary))

  /**
   * Find edges by traversing two k-mer lists, one sorted by the first
   * k-1 bases and the other sorted by the final k-1.
   */
  @tailrec
  def findEdges(byBeginning: List[KmerNode], byEnd: List[KmerNode], gotEdges: Boolean = false): Boolean = {
    byBeginning match {
      case b :: bs =>
        byEnd match {
          case e :: es =>
            val overlap = e.end
            val cmp = overlap.compareTo(b.begin)
            if (cmp < 0) {
              findEdges(byBeginning, es, gotEdges)
            } else if (cmp > 0) {
              findEdges(bs, byEnd, gotEdges)
            } else {
              val ns = byEnd.takeWhile(_.end == overlap)
              for (n <- ns) {
                result.addEdge(n, b)
              }
              //All possible edges for b have been constructed
              findEdges(bs, byEnd, true)
            }
          case _ => gotEdges
        }
      case _ => gotEdges
    }
  }

  def loadKmers(part: Iterable[MacroNode]) = {
    val partMap = Map() ++ part.map(x => x.uncompact -> (x.id, x))
    val bulkData = pathdb.getBulk(part.map(_.uncompact))

    Map() ++ (for {
      (key, bucket) <- bulkData
      macroNode = partMap(key)._2
      id = partMap(key)._1
      kmers = bucket.kmersBySequenceWithCoverage.toList.flatMap(x =>
        sequenceToKmerNodes(macroNode, x._1, x._2.iterator))
    } yield (id, kmers))
  }

  /**
   * Add marker set buckets to the path graph, constructing all edges between k-mers
   * in the buckets of this partition.
   */
  def addNodes(part: Iterable[MacroNode]) {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")


    val partMap = Map() ++ part.map(x => x.uncompact -> x.id)
    val partSet = part.toSet
    val kmers = loadKmers(part)

    result = new DoubleArrayListGraph[KmerNode](kmers.values.toList.flatten.toArray)

    val byStart = kmers.map(x => (x._1 -> x._2.sortBy(_.seq)))

    //Tracking bucket pairs with actual k-mer edges being constructed
    var foundEdges = 0

    for {
      fromBucket <- part
      byEnd = kmers(fromBucket.id).sortBy(_.end)
      toBuckets = macroGraph.edgesFrom(fromBucket).filter(partSet.contains)
      toBucket <- fromBucket :: toBuckets //Always add an edge back to the same bucket
    } {
      val added = findEdges(byStart(toBucket.id), byEnd)
      if (added) {
        foundEdges += 1
      }
    }
    println(s"$foundEdges bucket pairs have k-mer edges")
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
  def computePathCov(path: Iterable[N]): Double =
    path.map(p => p.coverage).sum.toDouble / path.size

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
          unsetNoise(paths.sortWith((p, q) => computePathCov(p) > computePathCov(q)).head)
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