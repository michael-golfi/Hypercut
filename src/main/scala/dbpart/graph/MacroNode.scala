package dbpart.graph

import dbpart.MarkerSet
import dbpart.MarkerSpace
import dbpart.HasID

final class MacroNode(val data: Array[Byte]) extends HasID {
  var inPartition: Boolean = false

  def uncompact(implicit space: MarkerSpace): String = MarkerSet.uncompactToString(data, space)

  import java.util.{ Arrays => JArrays }

  override def hashCode: Int = JArrays.hashCode(data)

  override def equals(other: Any): Boolean = other match {
    case mn: MacroNode =>
      JArrays.equals(data, mn.data)
    case _ => super.equals(other)
  }
}