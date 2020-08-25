package hypercut.hash

import scala.collection.mutable.Map

object FeatureCounter {
  def apply(space: MotifSpace): FeatureCounter = apply(space.byPriority.length)

  def apply(n: Int) = new FeatureCounter(new Array[Int](n))

  def toSpaceByFrequency(oldSpace: MotifSpace, counts: Array[(String, Int)], id: String) = {
    //This must define a total ordering, otherwise a given hash can't be reliably reproduced later
    new MotifSpace(
      counts.sortBy(x => (x._2, x._1)).map(_._1), oldSpace.n, id
    )
  }
}

/**
 * Counts motif occurrences (independently) in a dataset
 * to establish relative frequencies.
 */
final case class FeatureCounter(counter: Array[Int]) {

  def numMotifs: Int = counter.length

  def motifsWithCounts(space: MotifSpace) = space.byPriority zip counter

  def increment(motif: Motif, n: Int = 1) {
    val rank = motif.features.tagRank
    if (counter(rank) <= Int.MaxValue - n) {
      counter(motif.features.tagRank) += n
    } else {
      counter(rank) = Int.MaxValue
    }
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
    for (i <- counter.indices) {
      val inc = other.counter(i)
      if (counter(i) <= Int.MaxValue - inc) {
        counter(i) += inc
      } else {
        counter(i) = Int.MaxValue
      }
    }
  }

  /**
   * Operation only well-defined for counters based on the same motif space.
   * To avoid allocation of potentially big arrays,
   * mutates this object and returns it.
   * @param other
   * @return
   */
  def + (other: FeatureCounter): FeatureCounter = {
    this += other
    this
  }

  def sum: Long = counter.map(_.toLong).sum

  def print(space: MotifSpace, heading: String) {
    val s = sum
    def perc(x: Int) = "%.2f%%".format(x.toDouble/s * 100)

    println(heading)
    val first = (motifsWithCounts(space)).filter(_._2 > 0).take(20)
    println(s"Showing max 20/${counter.size} motifs")
    println(first.map(_._1).mkString("\t"))
    println(first.map(_._2).mkString("\t"))
    println(first.map(c => perc(c._2)).mkString("\t"))
  }

  /**
   * Construct a new motif space where the least common motifs in this counter
   * have the highest priority.
   * Other parameters (e.g. n) will be shared with the old space that this is based on.
   */
  def toSpaceByFrequency(oldSpace: MotifSpace, id: String): MotifSpace = {
    val pairs = motifsWithCounts(oldSpace)
    FeatureCounter.toSpaceByFrequency(oldSpace, pairs, id)
  }

}