package friedrich.util

object Distribution {   
  def printStats(label: String, vs: Iterable[Int]) {
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

  def observe(vs: Iterable[Int]) {
    for (v <- vs) {
      observe(v)
    }
  }
  
  def print() {
    println(s"Min $min Max $max Avg $avg Count $count Sum $sum")
  }
 
}