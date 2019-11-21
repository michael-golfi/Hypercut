package hypercut.hash

import scala.collection.mutable.Map

final case class FeatureCounter() {
  val counter: scala.collection.mutable.Map[String,Long] = Map.empty

  def increment(feature: String, n: Long = 1) {
    counter.get(feature) match {
      case Some(c) => counter(feature) = c + n
      case _ => counter += (feature -> n)
    }
  }

  def += (feature: String) = {
    increment(feature)
  }

  def += (ms: MarkerSet) {
    for (m <- ms.relativeMarkers) {
      increment(m.tag)
    }
  }

  def += (other: FeatureCounter) {
    for ((f, c) <- other.counter) {
      increment(f, c)
    }
  }

  def + (other: FeatureCounter) = {
    val r = new FeatureCounter
    r += this
    r += other
    r
  }

  def sum: Long = counter.toSeq.map(_._2).sum

  def print(heading: String) {
    val s = sum
    def perc(x: Long) = "%.2f%%".format(x.toDouble/s * 100)

    println(heading)
    println(counter.map(_._1).mkString("\t"))
    println(counter.map(_._2).mkString("\t"))
    println(counter.map(c => perc(c._2)).mkString("\t"))
  }

  /**
   * Construct a new marker space where the least common markers in this counter
   * have the highest priority.
   */
  def toSpaceByFrequency(n: Int) = {
    new MarkerSpace(counter.toSeq.sortBy(_._2).map(_._1).toArray, n)
  }

}