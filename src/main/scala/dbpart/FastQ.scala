package dbpart

import scala.io.BufferedSource
import friedrich.util.IO

object FastQ {
 def iterator(file: String): Iterator[String] =
   iterator(IO.source(file))

 def iterator(r: BufferedSource): Iterator[String] =
   r.getLines.grouped(4).map(_(1))
}