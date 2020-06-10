package hypercut.hash

import scala.collection.mutable.Map

/**
 * Counts motif occurrences (independently) in a dataset
 * to establish relative frequencies.
 */
final case class FeatureCounter(space: MotifSpace) {
  val counter = new Array[Long](space.byPriority.length)

  def motifsWithCounts = space.byPriority zip counter

  def increment(motif: Motif, n: Long = 1) {
    counter(motif.features.tagRank) += n
  }

  def += (motif: Motif) = {
    increment(motif)
  }

  def += (ms: MotifSet) {
    for (m <- ms.relativeMotifs) {
      increment(m)
    }
  }

  /**
   * Operation only well-defined for counters based on the same motif space.
   * @param other
   */
  def += (other: FeatureCounter) {
    var i = 0
    while (i < counter.length) {
      counter(i) += other.counter(i)
      i += 1
    }
  }

  /**
   * Operation only well-defined for counters based on the same motif space.
   * @param other
   * @return
   */
  def + (other: FeatureCounter) = {
    val r = new FeatureCounter(space)
    r += this
    r += other
    r
  }

  def sum: Long = counter.sum

  def print(heading: String) {
    val s = sum
    def perc(x: Long) = "%.2f%%".format(x.toDouble/s * 100)

    println(heading)
    val first = (motifsWithCounts).take(20)
    println(s"Showing max 20/${counter.size} motifs")
    println(first.map(_._1).mkString("\t"))
    println(first.map(_._2).mkString("\t"))
    println(first.map(c => perc(c._2)).mkString("\t"))
  }

  /**
   * Construct a new motif space where the least common motifs in this counter
   * have the highest priority.
   */
  def toSpaceByFrequency(n: Int, id: String) = {
    val pairs = motifsWithCounts
    new MotifSpace(pairs.sortBy(_._2).map(_._1), n, id)
  }

}