package dbpart


final class MarkerSetExtractor(space: MarkerSpace, numMarkers: Int, k: Int) {
  
    def topRanked(ms: Seq[Marker], n: Int) =
    (ms.sortBy(m => (space.priorityOf(m.tag), m.pos))
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
    var r = List[MarkerSet]()
    readCount += 1
    
    val markers = space.allMarkers(read)
    var start = 0    
    val motifLength = space.minMotifLength
    var (currentMarkers, remainingMarkers) = markers.span(_.pos <= k - motifLength)
    var byRank = topRanked(currentMarkers, n)
    var last = markerSetFromUnsorted(byRank)
    r ::= last
    
     while (start < read.size - k) {
      start += 1
      var (newPart, rem) = remainingMarkers.span(_.pos <= start + k - motifLength)
      if (!newPart.isEmpty || 
          (!currentMarkers.isEmpty && currentMarkers.head.pos < start)
          ) {
        remainingMarkers = rem
        currentMarkers = currentMarkers.dropWhile(_.pos < start) ++ newPart
        //rank drop

        byRank = topRanked(currentMarkers, n)
        val current = markerSetFromUnsorted(byRank)

        if (last != current) {
          r ::= current          
          last = current
        } else {
          r ::= last
        }        
      } else {
        r ::= last
      }
      kmerCount += 1
    }
    
    r.reverse
  }
}