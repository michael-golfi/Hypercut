package hypercut.taxonomic

import hypercut.hash.ReadSplitter
import hypercut.spark.HashSegment
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer

import scala.annotation.{switch, tailrec}

object HashSegments {
    /*
    * Includes an index, so that the ordering of the hashSegments can be reconstructed later.
    * Also includes ambiguous segments at the correct location.
    */
  def createHashSegmentsWithIndex[H, T](r: String, tag: T, splitter: ReadSplitter[H]): Iterator[(HashSegment, Int, T)] = {
    val all = splitByAmbiguity(r, splitter.k)
    val allSegs = all.flatMap(r => createSegments(r._1, r._3, splitter)).toList

    //Put the ambiguous segments in some hash bucket to spread them out. They will not be processed
    //so location irrelevant.
    //TODO this scheme needs to be improved if we get a lot of ambiguous data
    val unambigHash = allSegs.find(_.segment.data != null).map(_.hash).getOrElse(0L)
    allSegs.iterator.zipWithIndex.map(s => {
      if (s._1.segment.data == null) {
        (s._1.copy(hash = unambigHash), s._2, tag)
      } else {
        (s._1, s._2, tag)
      }
    })
  }

  def createSegments[H, T](r: String, ambiguous: Boolean, splitter: ReadSplitter[H]) = {
    if (ambiguous) {
      val numKmers = r.length - (splitter.k - 1)

      //can only represent lengths up to Short.MaxValue
      val lengths = (0 until (numKmers / Short.MaxValue)).toSeq.map(i => Short.MaxValue) :+
        ((numKmers % Short.MaxValue).toShort)

      lengths.iterator.map(l =>
        HashSegment(0, ZeroBPBuffer(null, l))
      )
    } else {
      for {
        (h, s) <- splitter.split(r)
        r = HashSegment(splitter.compact(h), BPBuffer.wrap(s))
      } yield r
    }
  }

  def isAmbiguous(c: Char) =
    (c: @switch) match {
      case 'C' | 'T' | 'A' | 'G' | 'U' => false
      case _ => true
    }

  /**
   * Split a read into maximally long fragments overlapping by (k-1) bases,
   * flagging those which contain ambiguous nucleotides.
   *
   * @return Tuples of fragments, their position, and the ambiguous flag
   *         (true if the fragment contains ambiguous nucleotides).
   */
  def splitByAmbiguity(r: String, k: Int): Iterator[(String, Int, Boolean)] =
    splitByAmbiguity(r, k, "", false, List()).iterator.
      filter(_._1.length >= k).zipWithIndex.map(x =>
      (x._1._1, x._2, x._1._2)
    )

  /**
   * Progressively split a read.
   *
   * @param r             Remaining subject to be classified. First character has not yet been judged to be
   *                      ambiguous/nonambiguous
   * @param k
   * @param building      Fragment currently being built (prior to 'r')
   * @param buildingAmbig Whether currently built fragment is ambiguous
   * @param acc
   * @return
   */
  @tailrec
  def splitByAmbiguity(r: String, k: Int, building: String, buildingAmbig: Boolean,
                       acc: List[(String, Boolean)] = List()): List[(String, Boolean)] = {
    if (r.isEmpty) {
      if (building.length > 0) {
        ((building, buildingAmbig) :: acc).reverse
      } else {
        acc.reverse
      }
    } else {
      val i = r.indexWhere(isAmbiguous)
      if (i < k && i != -1) {
        //Enter / stay in ambiguous mode
        splitByAmbiguity(r.substring(i + 1), k, building + r.substring(0, i + 1),
          true, acc)
      } else if (buildingAmbig && (i >= k || i == -1)) {
        val endPart = (if (r.length >= (k - 1)) r.substring(0, (k - 1)) else r)
        //Yield and switch to unambiguous
        splitByAmbiguity(r, k, "", false,
          ((building + endPart), true) :: acc)
      } else if (i >= k) { //!buildingAmbig
        //switch to ambiguous
        val splitAt = i - (k - 1)
        splitByAmbiguity(r.substring(i + 1), k, r.substring(splitAt, splitAt + k),
          true, (building + r.substring(0, i), false) :: acc)
      } else { //!buildingAmbig && i == -1
        ((building + r, false) :: acc).reverse
      }
    }
  }
}
