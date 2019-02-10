package dbpart


final class MarkerSetExtractor(space: MarkerSpace, numMarkers: Int, k: Int) {

    def topRanked(ms: Seq[Marker], n: Int) =
    (ms.sortBy(m => (m.features.tagRank, m.pos))
        take n)

   @volatile
   var readCount = 0
   @volatile
   var kmerCount = 0

   val n = numMarkers

  def markerSetFromUnsorted(ms: Seq[Marker]) =
    new MarkerSet(space, MarkerSet.relativePositions(space, ms)).fromZero

  final class MarkerExtractor(read: String) {
    var scannedToPos: Int = 0
    var markersByPos: List[Marker] = List()

    //Final position with the rank has the lowest priority
    def rankRemove(markers: List[Marker], rank: Int, amt: Int): List[Marker] = {
      if (amt == 0) {
        markers
      } else {
        val idx = markers.indexWhere(_.features.tagRank == rank)
        markers.take(idx) ::: rankRemove(markers.drop(idx + 1), rank, amt - 1)
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
      if (pos > scannedToPos + 1) {
        //Ensure we catch up
        scanTo(pos - 1)
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
                rankRemove(markersByPos.reverse, minRank, markersByPos.length - n).reverse
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

      val ext = new MarkerExtractor(read)
      for (p <- 0 until (k-1)) {
        ext.scanTo(p)
      }
      val r = ((k - 1) until (read.size - space.maxMotifLength)).toList.map(p => {
        kmerCount += 1
        new MarkerSet(space, MarkerSet.relativePositionsFromSorted(space, ext.scanTo(p))).fromZero
      })
//      println(s"Extracted $r")
      r
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
}