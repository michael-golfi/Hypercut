package hypercut.hash

import hypercut.NTSeq

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer


/**
 * Scans a single read, using mutable state to track the current motif set
 * in a window.
 */
final class WindowExtractor(space: MotifSpace, scanner: ShiftScanner,
                            windowMotifs: TopRankCache, k: Int, read: NTSeq) {
  val matches = scanner.allMatches(read)
  var scannedToPos: Int = space.maxMotifLength - 2

  def motifAt(pos: Int): Motif = matches(pos)

  /**
   * May only be called for monotonically increasing values of pos
   * pos is the final position of the window we scan to, inclusive.
   */
  def scanTo(pos: Int): List[Motif] = {
    while (pos > scannedToPos + 1) {
      //Catch up
      scanTo(scannedToPos + 1)
    }
    if (pos < scannedToPos) {
      throw new Exception("Invalid parameter, please supply increasing values of pos only")
    } else if (pos > scannedToPos) {
      //pos == scannedToPos + 1
      scannedToPos = pos
      if (pos >= read.length()) {
        throw new Exception("Already reached end of read")
      }
      val start = pos - k + 1
      val consider = pos - space.maxMotifLength

      if (consider >= 0) {
        windowMotifs :+= motifAt(consider)
      }
      windowMotifs.dropUntilPosition(start)
    }
    windowMotifs.takeByRank
  }
}

final case class MotifExtractor(space: MotifSpace, val k: Int) extends ReadSplitter[Motif] {
  @transient
  lazy val scanner = new ShiftScanner(space)

  @volatile
  var readCount = 0

  /**
   * Look for motif sets in a read.
   * Returns the positions where each contiguous MotifSet region is first detected
   */
  def motifSetsInRead(read: NTSeq): ArrayBuffer[(Motif, Int)] = {
    readCount += 1
    if (readCount % 100000 == 0) {
      println(s"$readCount reads seen")
    }

    if (read.length < k) {
      return ArrayBuffer.empty
    }

    val perBucket = new ArrayBuffer[(Motif, Int)](read.length)

    val ext = new WindowExtractor(space, scanner, new FastTopRankCache, k, read)
    ext.scanTo(k - 2)
    var p = k - 1

    var lastMotif: Motif = null

    while (p <= read.length - 1) {
      val scan = ext.scanTo(p).head
      if (!(scan eq lastMotif)) {
        lastMotif = scan
        perBucket += ((lastMotif, p))
      }
      p += 1
    }
    perBucket
  }

  def split(read: NTSeq): Iterator[(Motif, NTSeq)] = {
    val bkts = motifSetsInRead(read).toList
    SplitterUtils.splitRead(k, read, bkts).iterator
  }


  /**
   * Convert a hashcode into a compact representation.
   * @param hash
   * @return
   */
  def compact(hash: Motif): BucketId =
    hash.features.tagRank
}

object SplitterUtils {

  /**
   * Convert extracted buckets into overlapping substrings of a read,
   * overlapping by (k-1) bases. The ordering is not guaranteed.
   * Designed to operate on the second list produced by the motifSetsInRead function.
   */

  def splitRead[T](k: Int, read: NTSeq, buckets: List[(T, Int)]): List[(T, NTSeq)] =
    splitRead(k, read, buckets, Nil).reverse

  @tailrec
  def splitRead[T](k: Int, read: NTSeq, buckets: List[(T, Int)],
                acc: List[(T, NTSeq)]): List[(T, NTSeq)] = {
    buckets match {
      case b1 :: b2 :: bs =>
        splitRead(k, read, b2 :: bs, (b1._1, read.substring(b1._2 - (k - 1), b2._2)) :: acc)
      case b1 :: bs => (b1._1, read.substring(b1._2 - (k - 1))) :: acc
      case _ => acc
    }
  }
}