package hypercut.hash

import scala.collection.mutable.ArrayBuffer
import miniasm.genome.bpbuffer.BitRepresentation._


trait Scanner {
  def allMatches(data: String): ArrayBuffer[Motif]
}

/**
 * Bit-shift scanner for fixed width motifs.
 * @param width
 * @param space
 */
final class ShiftScanner(val space: MotifSpace) extends Scanner {

  assert(space.width <= 15)

  def width = space.maxMotifLength

  val mask: Int = {
    var r = 0
    var i = 0
    while (i < width) {
      r = (r << 2) | 3
      i += 1
    }
    r
  }

  val featuresByPriority = {
    space.byPriority.iterator.zipWithIndex.map(p => {
      new Features(p._1, p._2)
    }).toArray
  }


  /*
  Find all matches in the string.
  Returns an array with the matches in order.
 */
  def allMatches(data: String): ArrayBuffer[Motif] = {
    val r = new ArrayBuffer[Motif](data.length)
    var pos = 0
    var window: Int = 0
    while ((pos < width - 1) && pos < data.length) {
      window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
      pos += 1
    }

    while (pos < data.length) {
      window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
      val priority = space.priorityLookup(window)
      val motif = Motif(pos - (width - 1), featuresByPriority(priority))
      r += motif
      pos += 1
    }
    r
  }
}
