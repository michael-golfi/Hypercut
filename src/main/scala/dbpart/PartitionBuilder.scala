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

    var assignCount = 0
    val totalCount = graph.numNodes

    def partitions: List[Partition] = {
      for (n <- graph.nodes) {
        n.inPartition = false
      }

      var r = List[Partition]()
      for (n <- graph.nodes; if ! n.inPartition) {
        r ::= BFSfrom(n)
      }
      r
    }

    def BFSfrom(n: Node): List[Node] = {
      n.inPartition = true
      assignCount += 1
      BFSfrom(List(n), 1, List(n))
    }

    @tailrec
    final def BFSfrom(soFar: List[Node], soFarSize: Int, nextLevel: List[Node]): List[Node] = {
      if (soFarSize >= groupSize || (assignCount == totalCount)) {
        soFar
      } else {
        if (!nextLevel.isEmpty) {
          val next = nextLevel.flatMap(n =>
            (graph.edgesFrom(n) ++ graph.edgesTo(n)).filter(a => !a.inPartition))
          val need = groupSize - soFarSize
          val useNext = (next take need)
          for (un <- useNext) {
            un.inPartition = true
            assignCount += 1
          }
          BFSfrom(useNext.toList ::: soFar, soFarSize + useNext.size, useNext)
        } else {
          soFar
        }
      }
    }

    def degree(n: Node): Int = graph.fromDegree(n) + graph.toDegree(n)

    def DFSfrom(n: Node): List[Node] = {
      var soFarSize = 1
      var r: List[Node] = List(n)
      n.inPartition = true
      var stack: List[Node] = graph.edges(n).toList
      while (!stack.isEmpty && soFarSize < groupSize) {
        val o = stack.head
        stack = stack.tail
        if (!o.inPartition) {
          r ::= o
          o.inPartition = true
          soFarSize += 1
          //favour low-degree nodes in an effort to avoid branches
          //expensive.
          stack :::= graph.edges(o).toList.sortBy(degree)
        }
      }
      r
    }
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

