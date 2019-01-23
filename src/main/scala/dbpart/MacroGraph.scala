package dbpart

import friedrich.graph.Graph
import scala.collection.mutable.{ Set => MSet, Map => MMap }
import scala.annotation.tailrec
import dbpart.ubucket.UBucketDB

class MacroGraph(graph: Graph[MarkerSet]) {
  type Node = MarkerSet

  class Partitioner(groupSize: Int) {
    var remaining = MSet() ++ graph.nodes

    def partitions: List[List[Node]] = {
      for (n <- graph.nodes) {
        n.tag1 = false
      }

      var r = List[List[Node]]()
      while (!remaining.isEmpty) {
        val n = remaining.head
        n.tag1 = true
        remaining -= n
        val group = from(n, List(n), 1)
        r ::= group
      }
      r
    }

    @tailrec
    final def from(n: Node, soFar: List[Node], soFarSize: Int): List[Node] = {
      if (soFarSize >= groupSize || remaining.isEmpty) {
        soFar
      } else {
        val next = graph.edgesFrom(n).filter(a => !a.tag1)
        val need = groupSize - soFarSize
        val useNext = (next take need)

        for (un <- useNext) {
          remaining -= un
          un.tag1 = true
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
   * Group adjacent nodes in the graph.
   */
  def partition(groupSize: Int): List[List[Node]] =
    new Partitioner(groupSize).partitions



}

