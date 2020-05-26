package hypercut.hash

import miniasm.genome.bpbuffer.BitRepresentation

import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import scala.collection.Seq

object MotifSpace {
  def simple(n: Int) = new MotifSpace(Array(
        "TTA",
        "AT", "AC",
        "GT", "GC",
        "CTT"), n)

  def default(n: Int) = using(all2mers, n)

  val mixedTest = Seq(
    "ATA", "CCT", "AGG",
    "GT", "AC", "GC", "CC", "GG", "AT")

  val all1mers = Seq("A", "C", "T", "G")

  //Roughly in order from rare to common in an e.coli dataset
  val all2mers = Seq("AG", "CT", "GG", "CC",
    "GT", "AC", "GA", "TC",
    "CG", "GC",
    "TG", "CA", "AT", "TA",
    "TT", "AA")

  val all3mers = all2mers.flatMap(x => all1mers.map(y => x + y))
  val all4mers = all3mers.flatMap(x => all1mers.map(y => x + y))

  def named(name: String): Seq[String] = name match {
    case "all1" => all1mers
    case "all2" => all2mers
    case "all3" => all3mers
    case "all4" => all4mers
    case "mixedTest" => mixedTest
    case _ => throw new Exception(s"Unknown motif space name: $name")
  }

  def named(name: String, n: Int): MotifSpace = using(named(name), n)

  def using(mers: Seq[String], n: Int) = new MotifSpace(mers.toArray, n)
}

/**
 * A set of motifs that can be used, and their relative priorities.
 * @param n Number of motifs in a motif set.
 */
final case class MotifSpace(val byPriority: Array[String], val n: Int) {
  val maxMotifLength = byPriority.map(_.length()).max
  val minMotifLength = byPriority.map(_.length()).min

  val byFirstChar = byPriority.groupBy(_.charAt(0)).toMap

  def minPermittedStartOffset(motif: String) =
    maxMotifLength - motif.length()

  @volatile
  private var lookup = Map[String, Features]()

  def getFeatures(pattern: String): Features = {
    if (!lookup.contains(pattern)) {
      synchronized {
        val f = new Features(pattern, priorityOf(pattern))
        lookup += pattern -> f
      }
    }
    lookup(pattern)
  }

  def get(pattern: String, pos: Int): Motif = {
    Motif(pos, getFeatures(pattern))
  }

  def create(pattern: String, pos: Int): Motif = {
    Motif(pos, new Features(pattern, priorityOf(pattern)))
  }

  val priorityLookup = new Array[Int](256)
  for ((motif, pri) <- byPriority.zipWithIndex) {
    //Note: this approach can currently only handle motifs up to length 4,
    //but saves an expensive hash map lookup
    priorityLookup(BitRepresentation.quadToByte(motif) - Byte.MinValue) = pri
  }

  def priorityOf(mk: String) =
    priorityLookup(BitRepresentation.quadToByte(mk) - Byte.MinValue)

  @tailrec
  final def allIndexOf(s: String, ptn: String, from: Int, soFar: ListBuffer[Motif]) {
    if (from < s.length()) {
      val i = s.indexOf(ptn, from)
      if (i != -1) {
        soFar += get(ptn, i)
        allIndexOf(s, ptn, i + 1, soFar)
      }
    }
  }

  //Sort by position, inserting m2 into m1.
  //m1 has priority if there are position collisions.
  //TODO: make this tailrec
  private def mergeMotifs(m1: List[Motif], m2: List[Motif],
                           nextMotifAt: Int = 0): List[Motif] = {
    m1 match {
      case a :: ms =>
        if (m2.isEmpty) {
          m1
        } else {
          val m2h = m2.head
          if (a.pos <= m2h.pos) {
            //m1 has priority
            a :: mergeMotifs(m1.tail, m2, a.pos + 1)
            // && (m2h.pos + m2h.tag.length) <= a.pos
          } else if (m2h.pos >= nextMotifAt) {
            //Insert one motif from m2
              m2h :: mergeMotifs(m1, m2.tail, m2h.pos + 1)
            } else {
              //Drop one motif from m2
              mergeMotifs(m1, m2.tail, nextMotifAt)
            }
        }
      case _ => m2.dropWhile(_.pos < nextMotifAt)
    }
  }

  /**
   * All motifs, by absolute position
   */
  def allMotifs(s: String): List[Motif] = {
    var r = List[Motif]()
    val buf = ListBuffer[Motif]()
    buf.sizeHint(10)

    //TODO efficiency here when many motifs present
    //When a certain number of motifs have been found in every k-length window, there's no
    //point looking for any more. Iterative/incremental lookup?
    //Another source of potential efficiency increases is motifs that overlap, e.g. ATG and AT
    for (mark <- byPriority) {
      allIndexOf(s, mark, 0, buf)
      r = mergeMotifs(r, buf.toList)
      buf.clear()
    }
    r
  }
}