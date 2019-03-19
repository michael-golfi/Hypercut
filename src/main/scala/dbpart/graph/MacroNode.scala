package dbpart.graph

import dbpart.MarkerSpace
import dbpart.MarkerSet

final class MacroNode(val data: Array[Byte]) {
  var inPartition: Boolean = false

  def uncompact(implicit space: MarkerSpace): String = MarkerSet.uncompactToString(data, space)

  override def hashCode: Int = data.hashCode
}