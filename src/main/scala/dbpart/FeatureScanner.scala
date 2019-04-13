package dbpart

import dbpart.hash.FeatureCounter
import dbpart.hash.MarkerSpace
import dbpart.shortread.ReadFiles
import miniasm.genome.util.DNAHelpers
import dbpart.hash.Marker

final class FeatureScanner(val space: MarkerSpace, val k: Int) {

  def markerAt(read: String, pos: Int): Seq[Marker] = {
    //rely on these also being rank sorted
    val candidates = space.byFirstChar.get(read.charAt(pos))
    candidates match {
      case Some(cs) =>
        cs.filter(m => read.regionMatches(pos + 1, m, 1, m.length() - 1)).map(m => space.get(m, pos))
      case None => Seq()
    }
  }

  @volatile var readCount: Int = 0
  def scanRead(counter: FeatureCounter, read: String) {
    readCount += 1
    var i = 0
    val max = read.length - space.minMotifLength
    while (i < max) {
      for (m <- markerAt(read, i)) {
        counter += m.tag
      }
      i += 1
    }
  }

  def scanGroup(counter: FeatureCounter, rs: TraversableOnce[String]) {
    for (r <- rs) scanRead(counter, r)
  }

  def handle(reads: Iterator[String]): dbpart.hash.FeatureCounter = {
    val bufferSize = 100000
    val counter =
      reads.grouped(bufferSize).grouped(4).map(gg =>
        {
          println(s"$readCount reads seen")
          gg.par.map(rs => {
            val counter = new FeatureCounter
            val forward = scanGroup(counter, rs)
            val rev = scanGroup(counter, rs.map(DNAHelpers.reverseComplement))
            FeatureScanner.this.synchronized {
              counter.print("Scanned features")
            }
            counter
          }).seq.reduce(_ + _)
        }).reduce(_ + _)
    counter
  }

   def scan(inputFile: String, matesFile: Option[String]) {
    Stats.begin()
    val counter = handle(ReadFiles.iterator(inputFile))
    counter.print("Total feature count")
    Stats.end("Scan features")
    println("")
  }
}