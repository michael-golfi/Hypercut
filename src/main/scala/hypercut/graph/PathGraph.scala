package hypercut.graph

import scala.annotation.tailrec

import hypercut._
import hypercut.hash.MarkerSpace
import friedrich.graph.Graph
import miniasm.genome.util.DNAHelpers

/**
 * Simple PathGraphBuilder that constructs a graph from a predefined set of kmers.
 */
class PathGraphBuilder(k: Int) {

  def fromKmers(kmers: List[String]): Graph[KmerNode] = {
    //Currently setting all abundances to 1
    val nodes = kmers.map(s => new KmerNode(s, 1))
    fromNodes(nodes.toArray)
  }

  def fromNodes(nodes: Array[KmerNode]): Graph[KmerNode] = {
    val result = new DoubleArrayListGraph[KmerNode](nodes)
    val byStart = nodes.sortBy(_.seq)
    val byEnd = nodes.sortBy(_.end)

    val gotEdges = findEdges(byStart.toList, byEnd.toList, false,
      (f, t) => result.addEdge(f, t))
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
              val (sameEnd, nextE) = byEnd.span(_.end == overlap)
              val (sameBegin, nextB) = byBeginning.span(_.begin == overlap)
              for {
                from <- sameEnd
                to <- sameBegin
              } f(from, to)
              //All possible edges for b have been constructed
              findEdges(nextB, nextE, true, f)
            }
          case _ => gotEdges
        }
      case _ => gotEdges
    }
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
    g.nodes.filter(g.isBranchOut(_))

  /**
   * *
   * A list of nodes that have multiple backward edges.
   */
  def findBackwardJunctions(): Iterator[N] =
    g.nodes.filter(g.isBranchIn(_))

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

  def transform(list: List[List[N]]): List[List[N]] = transform(Nil, list)

  @tailrec
  def transform(acc: List[List[N]], list: List[List[N]]): List[List[N]] =
    list match {
      case (m :: ms) :: ls => {
        val ef = g.edgesFrom(m).filter(e => ! e.noise && ! ms.contains(e))
        if (ef.nonEmpty) {
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

    val junctions = findForwardJunctions().toSeq
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
          paths.tail.foldLeft(p.toSet)((a, b) => b.filter(a.contains(_)).toSet).nonEmpty
        case _ => false
      }
    }

  }
}