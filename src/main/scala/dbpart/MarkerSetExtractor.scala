package dbpart

import friedrich.util.IO
import scala.annotation.tailrec

final class MarkerSetExtractor(space: MarkerSpace, numMarkers: Int, k: Int) {
   @volatile
   var readCount = 0
   @volatile
   var kmerCount = 0

   val n = numMarkers

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
          map.find(m => read.substring(pos, pos + m.length()) == m).map(m => space.get(m, pos))
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
//      Thread.sleep(500)
//      println(s"Read $read")

      readCount += 1

      var r = List[MarkerSet]()
      val ext = new MarkerExtractor(read)
      ext.scanTo(k - 2)
      var p = k - 1

      while (p <= read.length - 1) {
        kmerCount += 1
        r ::= new MarkerSet(space, MarkerSet.relativePositionsSorted(space, ext.scanTo(p))).fromZero
        p += 1
      }
//      println(s"Extracted $r")
      r.reverse
    }

 /**
  * Extract bucket transitions (i.e. macro edges).
  */
  @tailrec
  def transitions(data: List[MarkerSet],
                  acc: List[ExpandedEdge] = Nil): List[ExpandedEdge] = {
    data match {
      case x :: y :: xs =>
        if (x.compact.toSeq == y.compact.toSeq) {
          transitions(y :: xs, acc)
        } else {
          transitions(y :: xs, (x, y) :: acc)
        }
      case _ => acc
    }
  }

  /**
   * Ingest a read.
   * Returns pairs of buckets and their k-mers, as well as bucket transitions.
   */
  def handle(read: String): (Iterator[(String, String)], List[ExpandedEdge]) = {
    val kmers = Read.kmers(read, k)

    val mss = markerSetsInRead(read)
    if (readCount % 10000 == 0) {
      synchronized {
        print(".")
      }
    }
    (mss.map(_.packedString).iterator zip kmers,
        transitions(mss))
  }

  def prettyPrintMarkers(input: String) = {
   val data = ReadFiles.iterator(input)
   for (read <- data) {
     print(s"Read: $read")
     val analysed = handle(read)
     var lastMarkers: String = ""
     for ((markers, kmer) <- analysed._1) {
       if (markers == lastMarkers) {
         print(s"$kmer ")
       } else {
         println("")
         lastMarkers = markers
         println(s"  $lastMarkers")
         print(s"    $kmer ")
       }
     }
     println("  Edges " + analysed._2.map(e =>
       e._1.packedString + "->" + e._2.packedString).mkString(" "))
     println("")
   }
  }

}