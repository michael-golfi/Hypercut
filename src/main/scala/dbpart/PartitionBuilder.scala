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
  val minSize = groupSize / 1.5

  def partitions: List[Partition] = {
    var r = List[Partition]()
    for (n <- graph.nodes; if !inPartition(n)) {
      r ::= bfsFrom(n)
      buildingID += 1
    }
    r
  }

  def assignToCurrent(n: Node) {
    if (!inPartition(n)) {
      assignCount += 1
      n.partitionId = buildingID
    }
  }

  def bfsFrom(n: Node): List[Node] = {
    assignToCurrent(n)
    bfsFrom(List(n), 1, List(n))
  }

  //Follows a non-branching path
  @tailrec
  def followNonBranchPath(n: Node, max : Int, acc: List[Node] = Nil): List[Node] = {
    assignToCurrent(n)

    val edges = graph.edgesFrom(n).filter(!inPartition(_)) :::
      graph.edgesTo(n).filter(!inPartition(_))

    if (edges.isEmpty) {
      n :: acc
    } else if (edges.size > 1 || max == 0 ) {
      n :: acc
    } else {
      followNonBranchPath(edges.head, max - 1, n :: acc)
    }
  }

  @tailrec
  def bfsFrom(soFar: List[Node], soFarSize: Int, nextLevel: List[Node]): List[Node] = {
    if (soFarSize >= groupSize || (assignCount == totalCount)) {
      refineBoundary(soFar)
      soFar
    } else {
      if (nextLevel.nonEmpty) {
        var next = MSet[Node]()
        val minNeed = minSize - soFarSize
        var partitionAdd = List[Node]()
        val need = groupSize - soFarSize

        var nonBranchTraversed = MSet[Node]()

        for {
          n <- nextLevel
          if graph.degree(n) == 2
          if (partitionAdd.size < need)
          path = followNonBranchPath(n, need)
        } {
          if (path.size > 1) {
            partitionAdd :::= path.dropRight(1)
            next += path.head
            nonBranchTraversed += n
          }
        }

        for {
          n <- nextLevel
          if (!nonBranchTraversed.contains(n))
          if ((next.size + partitionAdd.size) < need)
          edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
          newEdges = edges.filter(!inPartition(_))
        } {
          next ++= newEdges
        }
        for (un <- next) {
          assignToCurrent(un)
        }
        partitionAdd = (partitionAdd ++ next).distinct

        bfsFrom(partitionAdd ::: soFar, soFarSize + partitionAdd.size, next.toList)
      } else {
        refineBoundary(soFar)
        soFar
      }
    }
  }

  def dfsFrom(n: Node): List[Node] = {
    assignToCurrent(n)
    dfsFrom(List(n), 1, (graph.edgesFrom(n) ++ graph.edgesTo(n)).filter(_ != n))
  }

  @tailrec
  def dfsFrom(soFar: List[Node], soFarSize: Int, searchList: List[Node]): List[Node] = {
    if (soFarSize >= groupSize || (assignCount == totalCount)) {
      refineBoundary(soFar)
      soFar
    } else {
      searchList match {
        case n :: ns =>
          val edges = (graph.edgesFrom(n) ++ graph.edgesTo(n))
          val newEdges = edges.filter(a => !inPartition(a))
          if (newEdges.nonEmpty) {
            val use = newEdges.head
            assignToCurrent(use)
            dfsFrom(use :: soFar, soFarSize + 1, newEdges.tail ::: searchList)
          } else {
            dfsFrom(soFar, soFarSize, ns)
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
      fromOther = graph.edgesFrom(n).exists(a => a.partitionId != buildingID)
      toOther = graph.edgesTo(n).exists(a => a.partitionId != buildingID)
    } {
      if (!(fromOther || toOther)) {
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
        (building :: acc).filter(_.nonEmpty)
    }
  }

}
