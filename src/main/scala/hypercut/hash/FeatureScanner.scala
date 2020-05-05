package hypercut.hash

import hypercut.shortread.ReadFiles
import miniasm.genome.util.DNAHelpers

/**
 * Looks for raw motifs in reads, counting them in a histogram.
 */
final class FeatureScanner(val space: MotifSpace) {

  def motifTagAt(read: String, pos: Int): Iterator[String] = {
    //rely on these also being rank sorted
    val candidates = space.byFirstChar.get(read.charAt(pos))
    candidates match {
      case Some(cs) =>
        cs.iterator.filter(m => read.regionMatches(pos + 1, m, 1, m.length() - 1))
      case None => Iterator.empty
    }
  }

  @volatile var readCount: Int = 0
  def scanRead(counter: FeatureCounter, read: String) {
    readCount += 1
    var i = 0
    val max = read.length - space.minMotifLength
    while (i < max) {
      for (m <- motifTagAt(read, i)) {
        counter += m
      }
      i += 1
    }
  }

  def scanGroup(counter: FeatureCounter, rs: TraversableOnce[String]) {
    for (r <- rs) scanRead(counter, r)
  }

  def handle(reads: Iterator[String]): FeatureCounter = {
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
    val counter = handle(ReadFiles.iterator(inputFile))
    counter.print("Total feature count")
    println("In order from rare to common: ")
    println(counter.counter.toList.sortBy(_._2).map(_._1))
    println("")
  }
}