package dbpart

import scala.annotation.tailrec

object KmerSpace {
  type Edge = (MarkerSet, MarkerSet)
  
  def apply(elements: Iterable[MarkerSet]) = {
    val r = new KmerSpace
    for (e <- elements) {
      r.add(e)
    }
    r
  }

  /**
   * Find edges from s1 to s2 by gradually constraining both.
   * 
   * Invariant: s1 and s2 are validated up to, but not including,
   * their headPositions.
   */
  final def edges(s1: ConstrainedSpace, s2: ConstrainedSpace,
                  space: MarkerSpace,
                  n: Int,
                  mayRemoveLeftRank: Boolean = true,
                  mayInsertRight: Boolean = true,
                  removedLeftPos: Boolean = false,
                  mayAppendLeft: Boolean = false,
                  mustStop: Boolean = false): Iterator[Edge] = {

    var r = Iterator[Edge]()

//    println("Add leaves")
//    println(s1.headPosition + " " + s1.currentLeaves)
//    println(s2.headPosition + " " + s2.currentLeaves)

    r ++= (for (
      n1 <- s1.currentLeaves.toStream;
      n2 <- s2.currentLeaves;
      if (n1 != n2)
    ) yield (n1, n2))
      
    if (mustStop) {
      return r
    }
    
//    println(s"Step: ${s1.canStepForward} ${s2.canStepForward}")
    
    if (s1.canStepForward && s2.canStepForward) {      
      for (
        h1 <- s1.heads; h2 <- s2.heads;
        if (h1 equivalent h2); 
        s1b = s1.stepForward(h1); s2b = s2.stepForward(h2)
      ) {
//        println(s"Std case $h1 $h2")
//        println(s1b.toVector)
//        println(s2b.toVector)
        r ++= edges(s1b, s2b, space, n, mayRemoveLeftRank, mayInsertRight,
          removedLeftPos, mayAppendLeft) 
      }
    }
    
    if (!s1.canStepForward && s2.canStepForward && removedLeftPos) {
      for (h2 <- s2.heads;
        s2b = s2.stepForward(h2)) {
//        println(s"Right append case $h2")
        r ++= edges(s1, s2b, space, n, mayRemoveLeftRank, mayInsertRight,
          removedLeftPos, mayAppendLeft, true)
      }        
    }
    
    if (s1.canStepForward && !s2.canStepForward && mayAppendLeft) {
      for (h1 <- s1.heads; 
         s1b = s1.stepForward(h1)) {
        r ++= edges(s1b, s2, space, n, mayRemoveLeftRank, mayInsertRight,
          removedLeftPos, false) 
      }
    }
    
    if (s1.canStepForward && s2.canStepForward) {
      //Rank insert at arbitrary position - only happens if s1 was full
      if (mayInsertRight &&
        removedLeftPos) {
        val s1b = s1.atMinLength(n) //TODO correct?
        val s2b = s2.atMinLength(n)
        for (
          h1 <- s1b.heads; h2 <- s2b.heads;
          if (!(h1 equivalent h2));
          if (h1.sortValue < h2.sortValue)
        ) {
          
//          println(s"IR case $h1 $h2")
//          println(s1b.toVector)
//          println(s2b.toVector)
//          println(Seq(s1b.headPosition, s1b.lengthLower, s1b.lengthUpper, s1b).mkString(" "))
//          println(Seq(s2b.headPosition, s2b.lengthLower, s2b.lengthUpper, s2b).mkString(" "))

          val s2c = if (s2b.headPosition == 0) {
            s2b.stepForward(h2).setHeadsToZero(space)
          } else {
            s2b.constrainMarker(h2).dropAndAddOffset(space)
          }

          //s1 is only constrained here, but an append must happen
          //later on the left hand side to compensate.
          r ++= edges(s1b.constrainMarker(h1), s2c, space, n, mayRemoveLeftRank,
            false, removedLeftPos, true)
        }
      }
    }
    r
  }
}

/**
 * A set of k-mers, represented by marker sets, organised as a tree.
 * Suitable for partial and incremental lookups.
 */
final class KmerSpace {
  import KmerSpace._
  
  val root = new KmerTree(null)
  
  def add(ms: MarkerSet) = 
    root.add(ms.relativeMarkers, ms)
  
  def get(markers: Seq[Marker]) = 
    root.findLeaf(markers)
    
  def get(ms: MarkerSet) = 
    root.findLeaf(ms.relativeMarkers)
    
  def size = root.size
    
  def findSubspace(markers: Seq[Marker]): Option[KmerTree] = 
    root.findSubspace(markers)
    
  def getSubspace(marker: Marker): Option[KmerTree] =
    root.getSubspace(marker)
  
  def asConstrained = new ConstrainedSpace(root.allSubspaces.toVector)
  
   /**
   * Construct missing edges in the graph.
   * The sequences in a pair can vary substantially, with 4 basic cases at the start.
   * Equalities below refer to tags being equal.
   * 1. First markers are shared: s1(0) == s2(0) [both tag and pos]
   * 2. Position drop left: s1(1) == s2(0)
   *    Only possible if s1(0).pos == 0
   * 3. Rank insertion right crowds out s2(0), but no position drop left: s1(1) == s2(1)
   *    Only possible if s2(0) is lowestRank.
   * 4. Position drop left and rank insertion right: s1(1) == s2(2)
   *    Only possible if s2(0) is lowestRank, and s1(0).pos == 0
   */
  def completeEdges(space: MarkerSpace, n: Int): Iterator[Edge] = {
    val s1 = asConstrained
    val s2 = asConstrained
    
    //Normal case
    edges(s1, s2, space, n) ++ 
    //Case 2 and 4
    edges(s1.stepForward(0).setHeadsToZero(space), s2.setHeadsToZero(space), space, n, 
        false, true, true)     
  }
}

/**
 * A limited view of the k-mer space, constrained by conformance to some
 * (partial) marker sequence. Constraints can be applied gradually,
 * reducing the size of the space.
 * 
 * At each step, the heads are identical to the 'keys' of the current subspaces.
 * However, there can be multiple heads for each distinct key.
 * 
 * Unlike the KmerSpace and KmerTree, this class is immutable.
 * 
 * lower and upper bounds on length are applied lazily, at the time when
 * the entire space is iterated. 
 * They apply relative to the current tree depth. 
 * If lower <= 0 and upper >= 0, then leaves from the 
 * immediate subspaces may be used.
 * 
 * headPosition ranges from 0 (no markers stepped through) to n (max length of
 * MarkerSets) (all markers stepped through). currentLeaves corresponds to marker
 * sets of length 'headPosition' whose markers have all been stepped through.
 * 
 */
final case class ConstrainedSpace(subspaces: Iterable[KmerTree],
    headPosition: Int = 0,
    currentLeaves: Iterable[MarkerSet] = Seq(),
    lengthLower: Option[Int] = None, lengthUpper: Option[Int] = None) 
    extends Iterable[MarkerSet] {  
  
  lazy val subspaceLookup = subspaces.groupBy(_.key)
  
  import KmerTree._
  
  def iterator: Iterator[MarkerSet] = 
    subspaces.iterator.flatMap(_.all(lengthLower, lengthUpper))
  
  def heads: Iterable[Marker] = subspaceLookup.keys.toSeq
  
  /**
   * Leaves that have passed (all markers have been stepped
   * through)
   */
  def nextLeaves(from: Iterable[KmerTree]): Iterable[MarkerSet] = {
    if (lengthPass(lengthLower, lengthUpper)) {
      from.flatMap(_.leaves)
    } else {
      Seq()
    }
  }
  
  def canStepForward: Boolean = {
    !subspaces.isEmpty
  }
  
  private def stepDown(newSubspaces: Iterable[KmerTree],
                       newLeaves: Iterable[MarkerSet]) =
    new ConstrainedSpace(newSubspaces, headPosition + 1,
      newLeaves,
      lengthLower.map(_ - 1), lengthUpper.map(_ - 1))
  
  /**
   * Filter the subspaces and advance deeper.
   */
  def keyConstrainAdvance(key: Marker) = {
    val newSpaces = subspaceLookup(key)
    constrainAdvance(newSpaces)
  }
  
  /**
   * Filter the subspaces and advance deeper.
   */
  def filterConstrainAdvance(f: KmerTree => Boolean): ConstrainedSpace = {
   val newSpaces = subspaces.filter(f)
   constrainAdvance(newSpaces)
  }
  
  def constrainAdvance(newSpaces: Iterable[KmerTree]): ConstrainedSpace = {
   val sub = mergeSubspaces(newSpaces.flatMap(_.allSubspaces))   
   stepDown(sub, nextLeaves(newSpaces))   
  }
  
  private def mergeSubspaces(ss: Iterable[KmerTree]): Iterable[KmerTree] = 
    ss.groupBy(_.key).map(_._2.reduce(_ merge _))

  private def constrainSpaces(toSpaces: Iterable[KmerTree]) = {    
    copy(subspaces = toSpaces)
  }
  
  /**
   * Constrain without moving forward.
   */
  def constrainFilter(f: KmerTree => Boolean): ConstrainedSpace = 
    constrainSpaces(subspaces.filter(f))
  
  def mapHeads(f: Marker => Marker) =
    constrainSpaces(subspaces.map(_.mapHead(f)))
   
  def stepForward(constraint: Option[Marker]): ConstrainedSpace = 
    constraint match {
      case Some(c) => stepForward(c)
      case None => constrainAdvance(subspaces)
    }
  
  def stepForward(m: Marker): ConstrainedSpace = 
    keyConstrainAdvance(m)
  
  def constrainMarker(m: Marker): ConstrainedSpace = 
    constrainSpaces(subspaceLookup(m))
    
  def stepForward(constraint: Int): ConstrainedSpace = 
    filterConstrainAdvance(_.key.pos == constraint)
  
  def stepForward(constraint: String): ConstrainedSpace =
    filterConstrainAdvance(_.key.tag == constraint)
  
  def rankDrop(space: MarkerSpace): ConstrainedSpace =  
    constrainFilter(_.key.lowestRank == true).dropAndAddOffset(space)
  
  def setHeadsToZero(space: MarkerSpace) = mapHeads(
    m => space.get(m.tag, 0, m.lowestRank, m.sortValue))
  
  def dropAndSetToZero(space: MarkerSpace) =
    constrainAdvance(subspaces).setHeadsToZero(space)

  def dropAndAddOffset(space: MarkerSpace) = {
    val ns = subspaces.flatMap(s => {
      val n = s.key.pos
      s.allSubspaces.map(_.mapHead(m => 
        space.get(m.tag, m.pos + n, m.lowestRank, m.sortValue)))
    })
//    println("ns:? " + ns.map(x => x.key + ":" + x).mkString(","))
    stepDown(ns, nextLeaves(subspaces))
  }
  
  /**
   * All leaves at a certain depth or longer (number of markers)
   * The length is relative to the current head position of the space.
   */
  def atMinLength(leafLength: Int) = {
    val newMin = lengthLower match {
      case None => Some(leafLength - headPosition - 1)
      case Some(oldMin) => Some(Seq(oldMin, leafLength - headPosition -1).max)
    }
    copy(lengthLower = newMin,
      currentLeaves = currentLeaves.filter(_.relativeMarkers.size >= leafLength)
      )
  }
  
  def atMaxLength(leafLength: Int) = {
    if (lengthUpper != None) {
      throw new Exception("Overwriting length constraint")
    }
    copy(lengthUpper = Some(leafLength - headPosition - 1),
      currentLeaves = currentLeaves.filter(_.relativeMarkers.size <= leafLength)
      )
  }
  
  def atLength(leafLength: Int) = {
    if (lengthLower != None || lengthUpper != None) {
      throw new Exception(s"Overwriting length constraint. " +
        s"New: ${leafLength - headPosition - 1} Was: $lengthLower $lengthUpper")
    }
    copy(lengthLower = Some(leafLength - headPosition - 1), 
      lengthUpper = Some(leafLength - headPosition - 1),
      currentLeaves = currentLeaves.filter(_.relativeMarkers.size == leafLength)
      )
  }
  
  override def size: Int = 
    if (subspaces.isEmpty) { 0 } else (subspaces.map(_.size).sum)
  
  def breadth = subspaces.size
    
}

object KmerTree {
  def lengthPass(min: Option[Int], max: Option[Int]) = {
     val minPass = min match {
      case Some(d) => d <= 0
      case None => true
    }
    val maxPass = max match {
      case Some(d) => d >= 0
      case None => true
    }
    minPass && maxPass
  }
}

//key is null at the root only
//note that marker sets have variable length, e.g. 0 AT 11 AT can occur together with 0 AT 11 AT 2 CG
final class KmerTree(val key: Marker, var leaves: Set[MarkerSet] = Set())
  extends Iterable[MarkerSet] {
  import KmerTree._
  
  private var subspaces = Map[Marker, KmerTree]()
  
  //Merges two trees, which should have the same key and start from the same depth.
  def merge(other: KmerTree): KmerTree = {    
    var newSub = Map[Marker, KmerTree]()
    val keys = Set() ++ subspaces.keySet ++ other.subspaces.keySet
    for (k <- keys) {
      newSub += k -> merged(getSubspace(k), other.getSubspace(k))
    }
    val r = new KmerTree(key, leaves ++ other.leaves)
    r.subspaces = newSub
    r
  }
  
  def merged(t1: Option[KmerTree], t2: Option[KmerTree]): KmerTree = {
    (t1, t2) match {
      case (Some(t1), Some(t2)) => t1.merge(t2)
      case (Some(t1), _) => t1
      case (_, Some(t2)) => t2
      case _ => throw new Exception("Invalid merge of two empty trees")
    }
  }
  
  @tailrec
  def add(markers: Seq[Marker], leaf: MarkerSet): Unit = {
    markers.headOption match {
      case Some(m) =>
        subspaces.get(m) match {
          case Some(t) => t.add(markers.tail, leaf)
          case None =>
            val t = new KmerTree(m)
            subspaces += (m -> t)
            t.add(markers.tail, leaf)            
        }
      case None =>     
        this.leaves += leaf        
    }
  }
  
  /**
   * Get the subspace that corresponds to a single prefix item,
   * if any.
   */
  def getSubspace(marker: Marker): Option[KmerTree] = 
    subspaces.get(marker)

  /**
   * Find the subspace that corresponds to the given prefix sequence.
   */
  @tailrec
  def findSubspace(markers: Seq[Marker]): Option[KmerTree] = {
    markers.headOption match {
      case Some(m) =>
        subspaces.get(m) match {
          case Some(ss) => ss.findSubspace(markers.tail)
          case _ => None
        }
      case None => Some(this)
    }
  }

  def mapHead(f: Marker => Marker): KmerTree = {
    val kt = new KmerTree(f(key), leaves)
    kt.subspaces = subspaces
    kt
  }
  
  /**
   * Move forward one step in the MarkerSet sequence and get the subspaces at that step.
   */
  def allSubspaces: Iterator[KmerTree] = subspaces.valuesIterator
  
  @tailrec
  def findLeaf(markers: Seq[Marker]): Iterable[MarkerSet] = {
    markers.headOption match {
      case Some(m) => 
        subspaces.get(m) match {          
          case Some(t) => t.findLeaf(markers.tail)
          case _ => None
        }
      case None => leaves
    } 
  }
  
  override def size: Int = {
     leaves.size + 
       subspaces.values.map(_.size).sum
  }
  
  def iterator: Iterator[MarkerSet] = {
    val rest = subspaces.valuesIterator.flatMap(_.iterator)
    rest ++ leaves 
  }
  
  /**
   * Obtain markers at a certain depth (relative length).
   */
  def allAtLength(length: Int): Iterator[MarkerSet] = 
    all(Some(length), Some(length))
    
  
//  def allAtMinLength(length: Int): Iterator[MarkerSet] = {
//    def rest = subspaces.valuesIterator.flatMap(_.allAtMinLength(length - 1))
//    if (length <= 0) {
//      rest ++ leaf
//    } else {
//      rest
//    }  
//  }
  
  /**
   * Get all leaves, optionally bounding their length (position in the tree) above and/or below.
   */
  def all(minDepth: Option[Int], maxDepth: Option[Int]): Iterator[MarkerSet] = {
    def rest = subspaces.valuesIterator.flatMap(_.all(minDepth.map(_ - 1), maxDepth.map(_ - 1)))
    val minPass = minDepth match {
      case Some(d) => d <= 0
      case None => true
    }
    val maxPass = maxDepth match {
      case Some(d) => d >= 0
      case None => true
    }
    if (lengthPass(minDepth, maxDepth)) {    
      rest ++ leaves
    } else {
      rest
    }
    
  }
}
