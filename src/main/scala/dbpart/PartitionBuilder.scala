package dbpart

import friedrich.graph.Graph
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.annotation.tailrec
import dbpart.ubucket.BucketDB
import dbpart.graph.MacroNode

final class PartitionBuilder(graph: Graph[MacroNode]) {
  type Node = MacroNode
  type Partition = List[Node]

  final class Partitioner(groupSize: Int) {

    val partitionIds = Array.fill(graph.numNodes)(-1)
    def inPartition(n: MacroNode) = partitionIds(n.id) != -1

    var buildingID = 0
    var assignCount = 0
    val totalCount = graph.numNodes

    def partitions: List[Partition] = {
      var r = List[Partition]()
      for (n <- graph.nodes; if ! inPartition(n)) {
        r ::= BFSfrom(n)
        buildingID += 1
      }
      r
    }

    def assignToCurrent(n: Node) {
      assignCount += 1
      partitionIds(n.id) = buildingID
    }

    def BFSfrom(n: Node): List[Node] = {
      assignToCurrent(n)
      BFSfrom(List(n), 1, List(n))
    }

    @tailrec
    def BFSfrom(soFar: List[Node], soFarSize: Int, nextLevel: List[Node]): List[Node] = {
      if (soFarSize >= groupSize || (assignCount == totalCount)) {
        refineBoundary(soFar)
        soFar
      } else {
        if (!nextLevel.isEmpty) {
          var next: List[Node] = Nil
          val need = groupSize - soFarSize
          for {
            n <- nextLevel
            if (next.size < need)
            edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
            newEdges = edges.filter(a => !inPartition(a))
          } {
            next :::= newEdges
          }
          val useNext = next.distinct
          for (un <- useNext) {
            assignToCurrent(un)
          }
          BFSfrom(useNext.toList ::: soFar, soFarSize + useNext.size, useNext)
        } else {
          refineBoundary(soFar)
          soFar
        }
      }
    }

    def refineBoundary(partition: List[Node]) {
      for {
        n <- partition
        if n.isBoundary
        edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
        toOther = edges.filter(a => partitionIds(a.id) != buildingID)
      } {
        if (toOther.isEmpty) {
          n.isBoundary = false
        }
      }
    }

    def degree(n: Node): Int = graph.fromDegree(n) + graph.toDegree(n)

  }

  /**
   * Merge adjacent small partitions to increase the average size.
   * Very simple, dumb operation.
   */
  @tailrec
  final def collapse(groupSize: Int, partitions: List[Partition],
                     buildSize: Int = 0,
                     building: Partition = Nil,
                     acc: List[Partition] = Nil): List[Partition] = {
    partitions match {
      case p :: ps =>
        if (p.size + buildSize < groupSize) {
          collapse(groupSize, ps, buildSize + p.size, p ::: building, acc)
        } else if (p.size < groupSize) {
          //Go to next, try from p again
          collapse(groupSize, p :: ps, 0, Nil, building :: acc)
        } else {
          //Go to next, try after p
          collapse(groupSize, ps, 0, Nil, p :: building :: acc)
        }
      case _ =>
        //Stop here
        (building :: acc).filter(!_.isEmpty)
    }
  }

  /**
   * Group adjacent nodes in the graph.
   */
  def partition(groupSize: Int): List[Partition] =
    new Partitioner(groupSize).partitions

}

