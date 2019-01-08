package dbpart

object Stats {
  var lastTime = System.currentTimeMillis()
  
  def begin() {
    lastTime = System.currentTimeMillis()
  }
  
  def end(task: String) {
    val elapsed = System.currentTimeMillis() - lastTime
    val s = "%.2f".format(elapsed/1000.0)
    println(s"Completed $task in $s seconds.")
  }
}