package hypercut

import scala.collection.Seq
import hypercut.hash.MotifSet
import hypercut.hash.Motif
import hypercut.hash.MotifSpace

object Testing {

  val space = MotifSpace.simple(4)
  def m(code: String, pos: Int) = space.get(code, pos)
  def ms(motifs: Seq[(String, Int)]) = motifs.map(x => m(x._1, x._2))

}