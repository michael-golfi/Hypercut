package friedrich.util

object Distribution {
  def printStats(label: String, vs: TraversableOnce[Int]) {
    val d = new Distribution
    d.observe(vs)
    d.print(label)
  }
}

/**
 * Track some properties of a distribution of integers.
 */
class Distribution(format: String = "%.2f") {
  var max = 0L
  var count = 0L
  var avg = 0.0
  var sum = 0L
  var min = 1000L

  def observe(v: Int) {
    if (v < min) {
      min = v
    }
    if (v > max) {
      max = v
    }
    avg = (avg * count + v) / (count + 1)
    count += 1
    sum += v
  }

  def observe(vs: TraversableOnce[Int]) {
    for (v <- vs) {
      observe(v)
    }
  }

  def fmt(x: Double) = format.format(x)
  def print(title: String) {
    println(s"* * * $title Distribution")
    println(s"Min $min Max $max Avg ${fmt(avg)} Count $count Sum $sum")
  }
}

/**
 * Divide values into bins for the purposes of creating a histogram.
 * Note: might be useful to display certain histograms on a log scale
 * instead of linear
 */
class Histogram(values: Seq[Int], numBins: Int = 10,
  limitMax: Option[Long] = None) {
  val dist = new Distribution()
  dist.observe(values)

  val useMax = limitMax.getOrElse(dist.max)

  var bs = (useMax - dist.min + 1)/numBins
  if (bs < 1) {
    bs = 1
  }
  
  //Bins are upper bounds of values in each bin
  val bins = (dist.min + bs - 1).to(useMax, bs)
  var counts = Array.fill(numBins)(0)

  val mpos = values.size/2
  var srt = values.toArray.sorted
  val median = if (values.size > 0) Some(srt(mpos)) else None

  for (v <- values) {
    val i = bins.indexWhere(_ >= v)
    if (i != -1 && i < numBins) {
      counts(i) += 1
    } else {
      counts(numBins - 1) += 1
    } 
  }


  def print(label: String) {
    println(s"$label Median: $median ")
    dist.print(label)
    println(bins.take(numBins).mkString("\t"))
    println(counts.mkString("\t"))
  }

}