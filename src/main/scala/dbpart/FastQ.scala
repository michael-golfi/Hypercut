package dbpart

import java.io.BufferedReader
import scala.collection.JavaConversions._

object FastQ {
 def iterator(r: BufferedReader): Iterator[String] =
   r.lines().iterator.grouped(4).map(_(1))

}