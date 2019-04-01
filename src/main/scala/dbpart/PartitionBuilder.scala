package dbpart

import scala.annotation.tailrec
import scala.collection.mutable.{ Set => MSet }

import dbpart.graph.MacroNode
import friedrich.graph.Graph

final class PartitionBuilder(graph: Graph[MacroNode], groupSize: Int) {
  type Node = MacroNode
  type Partition = List[Node]

  def inPartition(n: MacroNode) = n.partitionId != -1

  var buildingID = 0
  var assignCount = 0
  val totalCount = graph.numNodes

  def partitions: List[Partition] = {
    var r = List[Partition]()
    for (n <- graph.nodes; if !inPartition(n)) {
      r ::= DFSfrom(n)
      buildingID += 1
    }
    r
  }

  def assignToCurrent(n: Node) {
    assignCount += 1
    n.partitionId = buildingID
  }

  def DFSfrom(n: Node): List[Node] = {
    assignToCurrent(n)
    DFSfrom(List(n), 1, (graph.edgesFrom(n) ++ graph.edgesTo(n)).filter(_ != n))
  }

  @tailrec
  def DFSfrom(soFar: List[Node], soFarSize: Int, searchList: List[Node]): List[Node] = {
    if (soFarSize >= groupSize || (assignCount == totalCount)) {
      refineBoundary(soFar)
      soFar
    } else {
      searchList match {
        case n :: ns =>
          val edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
          val newEdges = edges.filter(a => !inPartition(a))
          if (!newEdges.isEmpty) {
            val use = newEdges.head
            assignToCurrent(use)
            DFSfrom(use :: soFar, soFarSize + 1, newEdges.tail ::: searchList)
          } else {
            DFSfrom(soFar, soFarSize, ns)
          }
        case _ =>
          refineBoundary(soFar)
          soFar
      }
    }
  }

  def refineBoundary(partition: List[Node]) {
    for {
      n <- partition
      edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
      toOther = edges.filter(a => a.partitionId != buildingID)
    } {
      if (toOther.isEmpty) {
        n.isBoundary = false
      }
    }
  }

  def degree(n: Node): Int = graph.fromDegree(n) + graph.toDegree(n)

  /**
   * Merge adjacent small partitions to increase the average size.
   * Very simple, dumb operation.
   */
  @tailrec
  def collapse(groupSize: Int, partitions: List[Partition],
                     buildSize: Int             = 0,
                     building:  Partition       = Nil,
                     acc:       List[Partition] = Nil): List[Partition] = {
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

}

