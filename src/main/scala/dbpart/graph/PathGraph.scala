package dbpart.graph

import dbpart._
import dbpart.ubucket.SeqBucketDB
import dbpart.MarkerSet
import friedrich.graph.Graph
import dbpart.FastAdjListGraph
import scala.annotation.tailrec
import friedrich.util.formats.GraphViz

/**
 * Builds a graph that contains both inter-bucket and intra-bucket edges
 * between k-mers in a partition.
 */
class PathGraphBuilder(pathdb: SeqBucketDB,
    partitions: Iterable[Iterable[MacroNode]],
    macroGraph: Graph[MacroNode])(implicit space: MarkerSpace) {

  val k: Int = pathdb.k
  val result: Graph[KmerNode] = new DoublyLinkedGraph[KmerNode]

  for (p <- partitions) {
    addPartition(p)
  }

  def sequenceToKmerNodes(seqs: Iterator[String], covs: Iterator[Int]) =
    (seqs zip covs).toList.map(s => new KmerNode(s._1, s._2))


  /**
   * Add marker set buckets to the path graph, constructing all edges between k-mers
   * in the buckets of this partition.
   */
  def addPartition(part: Iterable[MacroNode]) {
//    println("Add partition: " + part.take(3).map(_.packedString).mkString(" ") +
//      s" ... (${part.size})")

    val partSet = part.toSet
    val bulkData = pathdb.getBulk(part.map(_.uncompact))
    val kmers = Map() ++ bulkData.
      map(x => (x._1 -> x._2.kmersBySequenceWithCoverage.map(x =>
        sequenceToKmerNodes(x._1, x._2.iterator))))

    for {
      subpart <- part
      sequence <-  kmers(subpart.uncompact)
    } {
      for { n <- sequence } { result.addNode(n) }
    }

    val byEnd = kmers.map(x => (x._1 -> x._2.flatten.groupBy(_.seq.takeRight(k - 1))))
//    val sorted = sequences.mapValues(_.toSeq.sorted)

    //Tracking bucket pairs with actual k-mer edges being constructed
    var foundEdges = Set[MacroEdge]()

    for {
      fromBucket <- part
      (overlap, fromSeqs) <- byEnd(fromBucket.uncompact)
      toInside = macroGraph.edgesFrom(fromBucket).iterator.filter(partSet.contains)
      toBucket <- toInside ++ Iterator(fromBucket)
    } {
      var added = false
      for {
        toKmer <- kmers(toBucket.uncompact).flatten.iterator.filter(_.seq.startsWith(overlap))
        fromKmer <- fromSeqs
      } {
        result.addEdge(fromKmer, toKmer)
        if (!added) {
          foundEdges += ((fromBucket, toBucket))
          added = true
        }
      }
    }
    println(s"${foundEdges.size} edges")
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