package hypercut.hash

import scala.annotation.tailrec

import hypercut._
import hypercut.shortread.ReadFiles
import hypercut.shortread.Read

final case class MotifSetExtractor(space: MotifSpace, k: Int) {
  @volatile
  var readCount = 0
  @volatile
  var kmerCount = 0

  val n = space.n

  @volatile
  lazy val scanner = new FSMScanner(space.byPriority)

  /**
   * Scans a single read, using mutable state to track the current motif set
   * in a window.
   */
  final class MotifExtractor(read: String) {
    var matches = scanner.allMatches(read).reverse
    var scannedToPos: Int = space.maxMotifLength - 2

    var windowMotifs = new TopRankCache(PosRankList(), n)

    def motifAt(pos: Int): Option[Motif] = {
      matches = matches.dropWhile(_._1 < pos)
      if (!matches.isEmpty && matches.head._1 == pos) {
        //Space.create allocates a new object each time, as opposed to space.get which
        //saves motifs and reuses them
        Some(space.create(matches.head._2, matches.head._1))
      } else {
        None
      }

      //rely on these also being rank sorted
//      val candidates = space.byFirstChar.get(read.charAt(pos))
//      candidates match {
//        case Some(map) =>
//          map.find(m => read.regionMatches(pos + 1, m, 1, m.length() - 1)).map(m => space.get(m, pos))
//        case None => None
//      }
    }

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

        //Position insert
        val consider = pos - space.maxMotifLength

        if (consider >= 0) {
          motifAt(consider) match {
            case Some(m) =>
              windowMotifs :+= m
            case None =>
          }
        }
        windowMotifs.dropUntilPosition(start, space)
      }
      //      println(windowMotifs)
      windowMotifs.takeByRank
    }
  }

  /**
   * Look for motif sets in a read.
   * Returns two lists:
   * 1. The MotifSet of every k-mer (in order),
   * 2. The positions where each contiguous MotifSet region is first detected
   */
  def motifSetsInRead(read: String): (List[MotifSet], List[(MotifSet, Int)]) = {
    readCount += 1
    if (readCount % 100000 == 0) {
      println(s"$readCount reads seen")
    }

    var perPosition = List[MotifSet]()
    var perBucket = List[(MotifSet, Int)]()

    val ext = new MotifExtractor(read)
    ext.scanTo(k - 2)
    var p = k - 1

    var lastMotifs: List[Motif] = null
    var lastMotifSet: MotifSet = null

    while (p <= read.length - 1) {
      kmerCount += 1
      val scan = ext.scanTo(p)
      if (!(scan eq lastMotifs)) {
        lastMotifSet = new MotifSet(space, MotifSet.relativePositionsSorted(space, scan)).fromZero
        lastMotifs = scan
        perBucket ::= (lastMotifSet, p)
      }
      perPosition ::= lastMotifSet
      p += 1
    }
    (perPosition.reverse, perBucket.reverse)
  }

  /**
   * Convert extracted buckets into overlapping substrings of a read,
   * overlapping by (k-1) bases. The ordering is not guaranteed.
   * Designed to operate on the second list produced by the motifSetsInRead function.
   */

  def splitRead(read: String, buckets: List[(MotifSet, Int)]): List[(MotifSet, String)] =
    splitRead(read, buckets, Nil).reverse

  @tailrec
  def splitRead(read: String, buckets: List[(MotifSet, Int)],
                acc: List[(MotifSet, String)]): List[(MotifSet, String)] = {
    buckets match {
      case b1 :: b2 :: bs =>
        splitRead(read, b2 :: bs, (b1._1, read.substring(b1._2 - (k - 1), b2._2)) :: acc)
      case b1 :: bs => (b1._1, read.substring(b1._2 - (k - 1))) :: acc
      case _ => acc
    }
  }

  /**
   * Ingest a read.
   * Returns the sequence of discovered buckets, as well as the k-mers in the read.
   * k-mers and buckets at the same position will correspond to each other.
   */
  def motifs(read: String): (List[MotifSet], Iterator[String]) = {
    val kmers = Read.kmers(read, k)
    val mss = motifSetsInRead(read)
    (mss._1, kmers)
  }

  /**
   * Ingest a read, returning pairs of discovered buckets (in compact form)
   * and corresponding k-mers.
   */
  def compactMotifs(read: String): List[(CompactNode, String)] = {
    val kmers = Read.kmers(read, k).toList
    val mss = motifSetsInRead(read)._1.map(_.compact)
    mss zip kmers
  }

  def prettyPrintMotifs(input: String) = {
    val data = ReadFiles.iterator(input)
    for (read <- data) {
      print(s"Read: $read")
      val analysed = motifs(read)
      var lastMotifs: String = ""
      for ((motifs, kmer) <- (analysed._1 zip analysed._2.toList)) {
        if (motifs.packedString == lastMotifs) {
          print(s"$kmer ")
        } else {
          println("")
          lastMotifs = motifs.packedString
          println(s"  $lastMotifs")
          print(s"    $kmer ")
        }
      }

      println("  Edges ")
      MotifSetExtractor.visitTransitions(analysed._1, (e, f) => print(
        e.packedString + " -> " + f.packedString + " "))
      println("")
    }
  }
}

object MotifSetExtractor {

  def fromSpace(spaceName: String, numMotifs: Int, k: Int) = {
    val space = MotifSpace.named(spaceName, numMotifs)
    new MotifSetExtractor(space, k)
  }

  /**
   * Efficiently visit bucket transitions (i.e. macro edges) in a list that was previously
   * computed by motifSetsInRead.
   */
  @tailrec
  def visitTransitions(data: List[MotifSet], f: (MotifSet, MotifSet) => Unit) {
    data match {
      case x :: y :: xs =>
        if ((x eq y) || (x.compact == y.compact)) {
          visitTransitions(y :: xs, f)
        } else {
          f(x, y)
          visitTransitions(y :: xs, f)
        }
      case _ =>
    }
  }

  @tailrec
  def collectTransitions[A](data: List[A], acc: List[(A, A)] = Nil): List[(A, A)] = {
    data match {
      case x :: y :: xs =>
        collectTransitions(y :: xs, (x, y) :: acc)
      case _ => acc
    }
  }
}