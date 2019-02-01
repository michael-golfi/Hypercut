package dbpart

import friedrich.graph.Graph
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.annotation.tailrec
import dbpart.ubucket.UBucketDB

class PartitionBuilder(graph: Graph[MarkerSet]) {
  type Node = MarkerSet
  type Partition = List[Node]

  class Partitioner(groupSize: Int) {

    var assignCount = 0
    val totalCount = graph.numNodes

    def partitions: List[Partition] = {
      //tag1 will indicate whether the node is in a partition
      for (n <- graph.nodes) {
        n.tag1 = false
      }

      var r = List[Partition]()
      for (n <- graph.nodes; if ! n.tag1) {
        n.tag1 = true
        assignCount += 1
        val group = from(n, List(n), 1)
        r ::= group
      }
      r
    }

    @tailrec
    final def from(n: Node, soFar: List[Node], soFarSize: Int): List[Node] = {
      if (soFarSize >= groupSize || (assignCount == totalCount)) {
        soFar
      } else {
        val next = graph.edgesFrom(n).filter(a => !a.tag1)
        val need = groupSize - soFarSize
        val useNext = (next take need)

        for (un <- useNext) {
          un.tag1 = true
          assignCount += 1
        }
        if (useNext.isEmpty) {
          soFar
        } else {
          from(useNext.head, useNext.toList ::: soFar, soFarSize + useNext.size)
        }
      }
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
        acc
    }
  }

  /**
   * Group adjacent nodes in the graph.
   */
  def partition(groupSize: Int): List[Partition] =
    new Partitioner(groupSize).partitions



}

