package dbpart.graph

import dbpart.hash.MarkerSet
import dbpart.hash.MarkerSpace
import dbpart.HasID

/**
 * A node in the macro graph. Corresponds to a bucket in the edge database.
 */
final class MacroNode(val data: Array[Byte]) extends HasID {
  /**
   * Whether this node is part of the partition boundary (outer edge)
   * and potentially has unknown edges into other partitions.
   */
  var isBoundary: Boolean = true

  var partitionId: Int = -1
  
  def uncompact(implicit space: MarkerSpace): String = MarkerSet.uncompactToString(data, space)

  import java.util.{ Arrays => JArrays }

  override def hashCode: Int = JArrays.hashCode(data)

  override def equals(other: Any): Boolean = other match {
    case mn: MacroNode =>
      JArrays.equals(data, mn.data)
    case _ => super.equals(other)
  }
}