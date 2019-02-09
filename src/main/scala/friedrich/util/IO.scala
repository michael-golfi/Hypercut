package friedrich.util

import java.util.zip._
import scala.io._
import java.io._

object IO {
  def trimKnownSuffixes(file: String): String = if (file.endsWith(".gz")) {
      file.substring(0, file.length() - 3)
    } else {
      file
    }

  /**
   * Intelligently obtain a file reader, transparently performing
   * transformations such as compression.
   */
  def source(file: String): BufferedSource = {
    if (file == "-") {
      Source.stdin
    } else {
      var stream: InputStream = new FileInputStream(file)
      if (file.endsWith(".gz")) {
        stream = new GZIPInputStream(stream)
      }
      new BufferedSource(stream)
    }
  }

   /**
   * Intelligently obtain a file writer, transparently performing
   * transformations such as compression.
   */
  def writer(file: String): BufferedWriter = {
    var stream: OutputStream = new FileOutputStream(file)
    if (file.endsWith(".gz")) {
      stream = new GZIPOutputStream(stream)
    }
    new BufferedWriter(new OutputStreamWriter(stream))
  }
}
