package dbpart

class StatTimer(val lastTime: Long) {
  def end(task: String) {
    val elapsed = System.currentTimeMillis() - lastTime
    val s = "%.2f".format(elapsed/1000.0)
    println(s"Completed $task in $s seconds.")
  }
}

object Stats {
  var last: StatTimer = _

  def beginNew: StatTimer = new StatTimer(System.currentTimeMillis())

  def begin() {
    last = beginNew
  }

  def end(task: String) {
    last.end(task)
  }
}