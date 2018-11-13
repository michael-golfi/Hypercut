package friedrich.util

object Distribution {   
  def printStats(label: String, vs: TraversableOnce[Int]) {
    println(s"Distribution for $label")
    val d = new Distribution
    d.observe(vs)
    d.print()
  }
}

/**
 * Track some properties of a distribution of integers.
 */
class Distribution {
  var max = 0
  var count = 0
  var avg = 0.0
  var sum = 0
  var min = 1000
  
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
  
  def print() {
    println(s"Min $min Max $max Avg $avg Count $count Sum $sum")
  }
}

class Histogram(values: Seq[Int]) {
  val dist = new Distribution()
  dist.observe(values)
  
  val bnum = 10
  val bs = (dist.max - dist.min)/bnum
  val buckets = dist.min.to(dist.max, bs)
  var counts = Array.fill(bnum)(0)
  
  val mpos = values.size/2
  var srt = values.sorted
  val median = srt(mpos)
  
  for (v <- values) {
    val i = buckets.indexWhere(_ >= v)
    if (i != -1 && i < bnum) {
      counts(i) += 1
    } else {
      counts(bnum - 1) += 1
    }    
  }
  
  
  def print(label: String) {
    println(s"$label Median: $median")
    println(buckets.take(bnum).mkString("\t"))
    println(counts.mkString("\t"))
  }
  
}