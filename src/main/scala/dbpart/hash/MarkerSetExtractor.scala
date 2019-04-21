package dbpart.hash

import scala.annotation.tailrec

import dbpart.shortread.ReadFiles
import dbpart.shortread.Read

final case class MarkerSetExtractor(space: MarkerSpace, k: Int) {
   @volatile
   var readCount = 0
   @volatile
   var kmerCount = 0

   val n = space.n

  /**
   * Scans a single read, using mutable state to track the current marker set
   * in a window.
   */
  final class MarkerExtractor(read: String) {
    var scannedToPos: Int = space.maxMotifLength - 2

    var windowMarkers = new TopRankCache(PosRankList(), n)

    def markerAt(pos: Int): Option[Marker] = {
      //rely on these also being rank sorted
      val candidates = space.byFirstChar.get(read.charAt(pos))
      candidates match {
        case Some(map) =>
          map.find(m => read.regionMatches(pos + 1, m, 1, m.length() - 1)).map(m => space.get(m, pos))
        case None => None
      }
    }

    /**
     * May only be called for monotonically increasing values of pos
     * pos is the final position of the window we scan to, inclusive.
     */
    def scanTo(pos: Int): List[Marker] = {
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
          markerAt(consider) match {
            case Some(m) =>
              windowMarkers :+= m
            case None =>
          }
        }
        windowMarkers.dropUntilPosition(start, space)
      }
//      println(windowMarkers)
      windowMarkers.takeByRank
    }
  }

    def markerSetsInRead(read: String): List[MarkerSet] = {
      readCount += 1
      if (readCount % 100000 == 0) {
        println(s"$readCount reads seen")
      }

      var r = List[MarkerSet]()
      val ext = new MarkerExtractor(read)
      ext.scanTo(k - 2)
      var p = k - 1

      var lastMarkers: List[Marker] = null
      var lastMarkerSet: MarkerSet = null

      while (p <= read.length - 1) {
        kmerCount += 1
        val scan = ext.scanTo(p)
        if (! (scan eq lastMarkers)) {
          lastMarkerSet = new MarkerSet(space, MarkerSet.relativePositionsSorted(space, scan)).fromZero
          lastMarkers = scan
        }
        r ::= lastMarkerSet
        p += 1
      }
      r.reverse
    }

  /**
   * Ingest a read.
   * Returns the sequence of discovered buckets, as well as the k-mers in the read.
   * k-mers and buckets at the same position will correspond to each other.
   */
  def markers(read: String): (List[MarkerSet], Iterator[String]) = {
    val kmers = Read.kmers(read, k)
    val mss = markerSetsInRead(read)
    (mss, kmers)
  }

  /**
   * Ingest a read, returning pairs of discovered buckets (in compact form)
   * and corresponding k-mers.
   */
  def compactMarkers(read: String): List[(CompactNode, String)] = {
    val kmers = Read.kmers(read, k).toList
    val mss = markerSetsInRead(read).map(_.compact)
    mss zip kmers
  }

  def prettyPrintMarkers(input: String) = {
   val data = ReadFiles.iterator(input)
   for (read <- data) {
     print(s"Read: $read")
     val analysed = markers(read)
     var lastMarkers: String = ""
     for ((markers, kmer) <- (analysed._1 zip analysed._2.toList)) {
       if (markers.packedString == lastMarkers) {
         print(s"$kmer ")
       } else {
         println("")
         lastMarkers = markers.packedString
         println(s"  $lastMarkers")
         print(s"    $kmer ")
       }
     }

     println("  Edges ")
     MarkerSetExtractor.visitTransitions(analysed._1, (e, f) => print(
       e.packedString + " -> " + f.packedString + " "))
     println("")
   }
  }
}

object MarkerSetExtractor {

  def fromSpace(spaceName: String, numMarkers: Int, k: Int) = {
    val space = MarkerSpace.named(spaceName, numMarkers)
    new MarkerSetExtractor(space, k)
  }

 /**
  * Efficiently visit bucket transitions (i.e. macro edges) in a list that was previously
  * computed by markerSetsInRead.
  */
  @tailrec
  def visitTransitions(data: List[MarkerSet], f: (MarkerSet, MarkerSet) => Unit) {
    data match {
      case x :: y :: xs =>
        if ((x eq y) || (x.compact == y.compact)) {
          visitTransitions(y :: xs, f)
        } else {
          f(x,y)
          visitTransitions(y :: xs, f)
        }
      case _ =>
    }
  }

  @tailrec
  def groupTransitions[T](data: List[(CompactNode, T)], acc: List[(CompactNode, List[T])] = Nil,
      acc2: List[T] = Nil): List[(CompactNode, List[T])] = {
    data match {
      case x :: y :: xs =>
        if ((x._1 eq y._1) || (x._1 == y._1)) {
          groupTransitions(y :: xs, acc, x._2 :: acc2)
        } else {
          groupTransitions(y :: xs, (x._1, x._2 :: acc2) :: acc, Nil)
        }
      case x:: xs =>
        (x._1, x._2 :: acc2) :: acc
      case _ =>
        acc
    }
  }
}