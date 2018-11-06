/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package friedrich.graph

import friedrich.graph._

import scala.collection.immutable.{TreeSet, HashMap, Set => ISet}
import scala.collection.mutable.{Set, MultiMap, HashMap => MHMap}
import scala.collection.{Set => CSet}

/**
 * A mutable graph that represents data internally as an adjacency list.
 * Adjacency lists map each node to a list of adjacent nodes.
 * 
 * TODO: override hasEdge 
 */
trait AdjListGraph[N] extends Graph[N] {

	private val adjList = new MHMap[N, Set[N]] with MultiMap[N, N]		
	private val revAdjList = new MHMap[N, Set[N]] with MultiMap[N, N]
	
	final override def edges(from: N): CSet[N] = edgesFrom(from) ++ edgesTo(from)
	
	final def edges(): Iterator[(N, N)] = nodes.flatMap( (n:N) => {
	  edgesFrom(n).map((x:N) => (n,x))
	})
	
	override def numEdges = nodes.foldLeft(0)(_ + edgesFrom(_).size)
	  
	def edgesFrom(from: N): CSet[N] = adjList.getOrElse(from, Set())
	def edgesTo(to: N): CSet[N] = revAdjList.getOrElse(to, Set())
	
	override def removeEdges(node: N) = {
	  adjList --= edgesFrom(node)
	  revAdjList --= edgesTo(node)
	}
	
	override def implAddEdge(from: N, to: N) {
	  addForward(from, to)
	  addBackward(to, from)
	}
	
	override def removeNode(node: N) = {
	  super.removeNode(node)
	  adjList.remove(node)
	  revAdjList.remove(node)
	}
	
	protected def addForward(from: N, to: N) {
	  adjList.addBinding(from, to)
	}
	
	protected def addBackward(from:N, to: N) {
	  revAdjList.addBinding(from, to)
	}
	
	protected def numForwardEdgeNodes = adjList.size

}

class SBAdjListGraph[N <: AnyRef] extends AdjListGraph[N] with SetBackedGraph[N]