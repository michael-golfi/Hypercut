package dbpart

import friedrich.util.IO

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

  /**
   * Extract the markers in a given read. Marker sets will be repeated according to how many
   * times they appeared in the input, so that for a given read length, the return list
   * will always have the same length.
   */
  def markerSetsInRead(read: String): List[MarkerSet] = {
//    println(s"\nRead $read")
    var r = List[MarkerSet]()
    readCount += 1

    val markers = space.allMarkers(read)
    var start = 0
    val motifLength = space.maxMotifLength
    var (currentMarkers, remainingMarkers) = markers.
      dropWhile(_.pos < motifLength - 1).
      span(_.pos <= k - motifLength)

    var byRank = topRanked(removeOverlaps(currentMarkers), n)
    var last = markerSetFromUnsorted(byRank)
    r ::= last
//    println(s"${last.packedString} for ${read.substring(start, k + start)}")

     while (start < read.size - k) {
      start += 1
      var (newPart, rem) = remainingMarkers.span(_.pos <= start + k - motifLength)
      if (!newPart.isEmpty ||
          (!currentMarkers.isEmpty && currentMarkers.head.pos < start + motifLength - 1)
          ) {

        remainingMarkers = rem
        currentMarkers = currentMarkers.dropWhile(_.pos < start + motifLength - 1) ++ newPart

        byRank = topRanked(removeOverlaps(currentMarkers), n)

        val current = markerSetFromUnsorted(byRank)

        //TODO this is currently always true, equality not implemented
        if (last != current) {
          r ::= current
          last = current
        } else {
          r ::= last
        }
      } else {
        r ::= last
      }
//      println(s"${last.packedString} for ${read.substring(start, k + start)}")
      kmerCount += 1
    }

    val res = r.reverse
//    println(s"Markers ${res.distinct.map(_.packedString)}")
    res
  }

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

    //TODO: mate pairs

  def handle(read: String) = {
    val kmers = Read.kmers(read, k)

    val mss = markerSetsInRead(read)
    if (readCount % 10000 == 0) {
      synchronized {
        print(".")
      }
    }
    mss.map(_.packedString).iterator zip kmers
  }

  def prettyPrintMarkers(input: String) = {
   val data = FastQ.iterator(input)
   for (read <- data) {
     print(s"Read: $read")
     val analysed = handle(read)
     var lastMarkers: String = ""
     for ((markers, kmer) <- analysed) {
       if (markers == lastMarkers) {
         print(s"$kmer ")
       } else {
         println("")
         lastMarkers = markers
         println(s"  $lastMarkers")
         print(s"    $kmer ")
       }
     }
     println("")
   }
  }

}