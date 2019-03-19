package dbpart.graph

import dbpart.MarkerSpace
import dbpart.MarkerSet

final class MacroNode(val data: Array[Byte]) {
  var inPartition: Boolean = false

  def uncompact(implicit space: MarkerSpace): String = MarkerSet.uncompactToString(data, space)

  import java.util.{Arrays => JArrays}

  override def hashCode: Int = JArrays.hashCode(data)

  override def equals(other: Any): Boolean = other match {
    case mn: MacroNode =>
      JArrays.equals(data, mn.data)
    case _ => super.equals(other)
  }
}