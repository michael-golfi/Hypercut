package dbpart.graph
import dbpart._

final class PathNode(val seq: NTSeq) {
  var seen: Boolean = false
}