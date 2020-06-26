package hypercut.hash

import hypercut.shortread.ReadFiles
import miniasm.genome.util.DNAHelpers
import scala.collection.mutable

/**
 * Looks for raw motifs in reads, counting them in a histogram.
 */
final class FeatureScanner(val space: MotifSpace) extends Serializable {
  @transient
  lazy val scanner = new ShiftScanner(space)

  @volatile var readCount: Int = 0
  def scanRead(counter: FeatureCounter, read: String) {
    readCount += 1
    for { m <- scanner.allMatches(read) } {
      counter += m
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
            val counter = FeatureCounter(space)
            val forward = scanGroup(counter, rs)
            val rev = scanGroup(counter, rs.map(DNAHelpers.reverseComplement))
            FeatureScanner.this.synchronized {
              counter.print(space, "Scanned features")
            }
            counter
          }).seq.reduce(_ + _)
        }).reduce(_ + _)
    counter
  }

  def scan(inputFile: String) {
    val counter = handle(ReadFiles.iterator(inputFile))
    counter.print(space, "Total feature count")
    println("In order from rare to common (first 20): ")
    println(counter.motifsWithCounts(space).sortBy(_._2).
      take(20).toList.map(_._1))
    println("")
  }
}