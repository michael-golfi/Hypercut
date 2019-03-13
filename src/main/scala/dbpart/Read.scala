package dbpart

object Read {
  /**
   * Extract all k-mers from a read.
   */
  def kmers(data: String, k: Int): Iterator[String] = {
    new Iterator[String] {
      val len = data.length()
      var at = 0
      def hasNext = at <= len - k

      def next = {
        val r = data.substring(at, at + k)
        at += 1
        r
      }
    }
  }
}