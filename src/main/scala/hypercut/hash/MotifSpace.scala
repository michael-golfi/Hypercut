package hypercut.hash

import hypercut.NTSeq
import miniasm.genome.bpbuffer.{BPBuffer, BitRepresentation}

import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import scala.collection.Seq

object MotifSpace {
  def simple(n: Int) = named("all2", n)

  val all1mersDNA = Seq("A", "C", "T", "G")
  val all1mersRNA = Seq("A", "C", "U", "G")

  val all2mers = Seq("AT", "AG", "CT", "GG", "CC",
    "AC", "GT", "GA", "TC",
    "CG", "GC",
    "TG", "CA", "TA",
    "TT", "AA")

  val all3mers = all2mers.flatMap(x => all1mersDNA.map(y => x + y))
  val all4mers = all3mers.flatMap(x => all1mersDNA.map(y => x + y))

  def namedMotifs(name: String): Seq[String] = name match {
    case "all1" => all1mersDNA
    case "all2" => all2mers
    case "all3" => all3mers
    case "all4" => all4mers
    case _ => throw new Exception(s"Unknown motif space name: $name")
  }

  def motifsOfLength(length: Int, rna: Boolean = false): Seq[String] = {
    val bases = if (rna) all1mersRNA else all1mersDNA
    if (length == 1) {
      bases
    } else if (length > 1) {
      motifsOfLength(length - 1, rna).flatMap(x => bases.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def named(name: String, n: Int): MotifSpace = using(namedMotifs(name), n, name)

  def ofLength(w: Int, n: Int, rna: Boolean, id: String): MotifSpace = using(motifsOfLength(w, rna), n, id)

  def using(mers: Seq[String], n: Int, id: String) = new MotifSpace(mers.toArray, n, id)
}

/**
 * A set of motifs that can be used, and their relative priorities.
 * @param n Number of motifs in a motif set.
 * @param width Max width of motifs in bps.
 * @param id To help distinguish this space from other spaces, for quick equality check
 */
final case class MotifSpace(val byPriority: Array[NTSeq], val n: Int, id: String) {
  val width = byPriority.map(_.length()).max
  def maxMotifLength = width
  val minMotifLength = byPriority.map(_.length()).min

  def minPermittedStartOffset(motif: NTSeq) =
    maxMotifLength - motif.length()

  @volatile
  private var lookup = Map.empty[NTSeq, Features]

  def getFeatures(pattern: NTSeq): Features = {
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

  def get(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, getFeatures(pattern))
  }

  def create(pattern: NTSeq, pos: Int): Motif = {
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
  def motifToInt(m: NTSeq) = {
    val wrapped = BPBuffer.wrap(m)
    BPBuffer.computeIntArrayElement(wrapped.data, 0, width.toShort, 0) >>> shift
  }

  val priorityLookup = new Array[Int](maxMotifs)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    priorityLookup(motifToInt(motif)) = pri
  }

  def priorityOf(mk: NTSeq) =
    priorityLookup(motifToInt(mk))

  val compactBytesPerMotif = if (width > 8) 4 else if (width > 4) 2 else 1

  /**
   * Size of a compact motif set in bytes
   * For each motif, 1 byte for offset, some number of bytes for the motif itself
   */
  val compactSize = (1 + compactBytesPerMotif) * n

}