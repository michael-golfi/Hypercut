package dbpart

import friedrich.graph.Graph
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.annotation.tailrec
import dbpart.ubucket.BucketDB

final class PartitionBuilder(graph: Graph[MarkerSet]) {
  type Node = MarkerSet
  type Partition = List[Node]

  final class Partitioner(groupSize: Int) {

    var assignCount = 0
    val totalCount = graph.numNodes

    def partitions: List[Partition] = {
      //tag1 will indicate whether the node is in a partition
      for (n <- graph.nodes) {
        n.inPartition = false
      }

      var r = List[Partition]()
      for (n <- graph.nodes; if ! n.inPartition) {
//        n.tag1 = true
//        assignCount += 1
//        r ::= BFSfrom(n, List(n), 1)\
        val part = growFrom(n, groupSize)
        r ::= part
      }
      r
    }

    def degree(n: Node): Int = graph.fromDegree(n) + graph.toDegree(n)

    def growFrom(n: Node, maxSize: Int): List[Node] = {
      var soFarSize = 1
      var r: List[Node] = List(n)
      n.inPartition = true
      var stack: List[Node] = graph.edges(n).toList
      while (!stack.isEmpty && soFarSize < maxSize) {
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
        acc
    }
  }

  /**
   * Group adjacent nodes in the graph.
   */
  def partition(groupSize: Int): List[Partition] =
    new Partitioner(groupSize).partitions



}

