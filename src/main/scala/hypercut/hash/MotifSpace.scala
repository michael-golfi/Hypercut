package hypercut.hash

import miniasm.genome.bpbuffer.{BPBuffer, BitRepresentation}

import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import scala.collection.Seq

object MotifSpace {
  def simple(n: Int) = new MotifSpace(Array(
        "TTA",
        "AT", "AC",
        "GT", "GC",
        "CTT"), n, 3)

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

  def namedMotifs(name: String): Seq[String] = name match {
    case "all1" => all1mers
    case "all2" => all2mers
    case "all3" => all3mers
    case "all4" => all4mers
    case "mixedTest" => mixedTest
    case _ => throw new Exception(s"Unknown motif space name: $name")
  }

  def motifsOfLength(length: Int): Seq[String] = {
    if (length == 1) all1mers
    else if (length > 1) {
      motifsOfLength(length - 1).flatMap(x => all1mers.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def named(name: String, n: Int): MotifSpace = using(namedMotifs(name), n, 4)

  def ofLength(w: Int, n: Int): MotifSpace = using(motifsOfLength(w), n, w)

  def using(mers: Seq[String], n: Int, width: Int) = new MotifSpace(mers.toArray, n, width)
}

/**
 * A set of motifs that can be used, and their relative priorities.
 * @param n Number of motifs in a motif set.
 * @param width Max width of motifs in bps.
 */
final case class MotifSpace(val byPriority: Array[String], val n: Int, val width: Int) {
  val maxMotifLength = byPriority.map(_.length()).max
  val minMotifLength = byPriority.map(_.length()).min

  val byFirstChar = byPriority.groupBy(_.charAt(0)).toMap

  def minPermittedStartOffset(motif: String) =
    maxMotifLength - motif.length()

  @volatile
  private var lookup = Map.empty[String, Features]

  def getFeatures(pattern: String): Features = {
    if (!lookup.contains(pattern)) {
      synchronized {
        if (!lookup.contains(pattern)) {
          val f = new Features(pattern, priorityOf(pattern))
          lookup += pattern -> f
        }
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

  //4 ^ width
  val maxMotifs = 4 << (width * 2 - 2)

  //bit shift distance
  val shift = 32 - (width * 2)

  /**
   * Compute lookup index for a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * Note: this mechanism, and the ones using it below, will not support mixed-length motifs.
   * @param m
   * @return
   */
  def motifToInt(m: String) = {
    val wrapped = BPBuffer.wrap(m)
    BPBuffer.computeIntArrayElement(wrapped.data, 0, width.toShort, 0) >>> shift
  }

  val priorityLookup = new Array[Int](maxMotifs)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    priorityLookup(motifToInt(motif)) = pri
  }

  def priorityOf(mk: String) =
    priorityLookup(motifToInt(mk))

  val compactBytesPerMotif = if (width > 8) 4 else if (width > 4) 2 else 1

  /**
   * Size of a compact motif set in bytes
   * For each motif, 1 byte for offset, some number of bytes for the motif itself
   */
  val compactSize = (1 + compactBytesPerMotif) * n

}