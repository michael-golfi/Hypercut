package hypercut.graph

trait HasID {
  var id: Int = 0
}

/**
 * A scheme for assigning integer IDs to a family of objects.
 */
class IDSpace[T <: HasID](val members: Array[T]) extends Serializable {
  for (x <- 0 until members.length) {
    members(x).id = x
  }
}