package dbpart

import friedrich.util.IO
import scala.annotation.tailrec

final class MarkerSetExtractor(space: MarkerSpace, numMarkers: Int, k: Int) {

   def topNByRankAndPos(ms: List[Marker], n: Int, fromRank: Int = 0) = {
     var useRank = fromRank
     var (atRank, others) = ms.partition(_.features.tagRank == useRank)
     var r = atRank.sortBy(_.pos)
     while (r.size < n && !others.isEmpty) {
       useRank += 1
       var (nAtRank, nOthers) = others.partition(_.features.tagRank == useRank)
       r ++= nAtRank.sortBy(_.pos)
       others = nOthers
     }
     r.take(n)
   }

   def topRanked(ms: List[Marker], n: Int) =
     topNByRankAndPos(ms, n)

   @volatile
   var readCount = 0
   @volatile
   var kmerCount = 0

   val n = numMarkers

  def markerSetFromUnsorted(ms: List[Marker]) =
    new MarkerSet(space, MarkerSet.relativePositions(space, ms)).fromZero

  final class MarkerExtractor(read: String) {
    var scannedToPos: Int = space.maxMotifLength - 2
    var markersByPos: List[Marker] = List()

    //Final position with the rank has the lowest priority.
    //Reverses the list.
    @tailrec
    def rankRemove(markers: List[Marker], rank: Int, amt: Int, build: List[Marker] = Nil): List[Marker] = {
      if (amt == 0) {
        markers.reverse ::: build
      } else {
        val h = markers.head
        if (h.features.tagRank == rank) {
          rankRemove(markers.tail, rank, amt - 1, build)
        } else {
          rankRemove(markers.tail, rank, amt, h :: build)
        }
      }
    }

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
      } else if (pos == scannedToPos) {
        markersByPos
      } else {
        //pos == scannedToPos + 1
        scannedToPos = pos
        if (pos >= read.length()) {
          throw new Exception("Already reached end of read")
        }
        val start = pos - k + 1
        if (!markersByPos.isEmpty && markersByPos.head.pos < start) {
          markersByPos = markersByPos.tail
        }
        val consider = pos - space.maxMotifLength
        if (consider < 0) {
          return markersByPos
        }
        markerAt(consider) match {
          case Some(m) =>
            markersByPos :+= m
            markersByPos = removeOverlaps(markersByPos)
            while (markersByPos.length > n) {
              val minRank = markersByPos.map(_.features.tagRank).min
              markersByPos =
                rankRemove(markersByPos.reverse, minRank, markersByPos.length - n)
            }
          case None =>
        }
        markersByPos
      }
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

      while (p <= read.length - space.maxMotifLength) {
        kmerCount += 1
        r ::= new MarkerSet(space, MarkerSet.relativePositionsSorted(space, ext.scanTo(p), Nil)).fromZero
        p += 1
      }
//      println(s"Extracted $r")
      r.reverse
    }

  /**
   * Extract the markers in a given read. Marker sets will be repeated according to how many
   * times they appeared in the input, so that for a given read length, the return list
   * will always have the same length.
   */
//  def markerSetsInRead(read: String): List[MarkerSet] = {
////    println(s"\nRead $read")
//    var r = List[MarkerSet]()
//    readCount += 1
//
//    val markers = space.allMarkers(read)
//    var start = 0
//    val motifLength = space.maxMotifLength
//    var (currentMarkers, remainingMarkers) = markers.
//      dropWhile(_.pos < motifLength - 1).
//      span(_.pos <= k - motifLength)
//
//    var byRank = topRanked(removeOverlaps(currentMarkers), n)
//    var last = markerSetFromUnsorted(byRank)
//    r ::= last
////    println(s"${last.packedString} for ${read.substring(start, k + start)}")
//
//     while (start < read.size - k) {
//      start += 1
//      var (newPart, rem) = remainingMarkers.span(_.pos <= start + k - motifLength)
//      if (!newPart.isEmpty ||
//          (!currentMarkers.isEmpty && currentMarkers.head.pos < start + motifLength - 1)
//          ) {
//
//        remainingMarkers = rem
//        currentMarkers = currentMarkers.dropWhile(_.pos < start + motifLength - 1) ++ newPart
//
//        byRank = topRanked(removeOverlaps(currentMarkers), n)
//
//        val current = markerSetFromUnsorted(byRank)
//
//        //TODO this is currently always true, equality not implemented
//        if (last != current) {
//          r ::= current
//          last = current
//        } else {
//          r ::= last
//        }
//      } else {
//        r ::= last
//      }
////      println(s"${last.packedString} for ${read.substring(start, k + start)}")
//      kmerCount += 1
//    }
//
//    val res = r.reverse
////    println(s"Markers ${res.distinct.map(_.packedString)}")
//    res
//  }

    //TODO make tailrec
    //Assumes markers are sorted by position.
    def removeOverlaps(markers: List[Marker]): List[Marker] = {
      markers match {
        case m1 :: m2 :: ms => {
          if (m1.pos + m1.tag.length <= m2.pos) {
            m1 :: removeOverlaps(m2 :: ms)
          } else {
            val p1 = m1.features.tagRank
            val p2 = m2.features.tagRank
            if (p1 <= p2) {
              removeOverlaps(m1 :: ms)
            } else {
              removeOverlaps(m2 :: ms)
            }
          }

        }
        case _ => markers
      }
    }

 /**
  * Extract bucket transitions (i.e. macro edges).
  */
  @tailrec
  def transitions(data: List[MarkerSet],
                  acc: List[MacroEdge] = Nil): List[MacroEdge] = {
    data match {
      case x :: y :: xs =>
        if (x.packedString == y.packedString) {
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
  def handle(read: String): (Iterator[(String, String)], List[MacroEdge]) = {
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
   val data = FastQ.iterator(input)
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