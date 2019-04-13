package dbpart.hash

import scala.collection.mutable.Map

final class FeatureCounter {
  val counter: scala.collection.mutable.Map[String,Long] = Map[String, Long]()

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

}