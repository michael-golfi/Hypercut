package hypercut

import scala.collection.Seq
import hypercut.hash.MotifSet
import hypercut.hash.Motif
import hypercut.hash.MotifSpace

object Testing {

  val space = MotifSpace.simple(4)
  def m(code: String, pos: Int) = space.get(code, pos)
  def ms(motifs: Seq[(String, Int)]) = motifs.map(x => m(x._1, x._2))

  def fixedMotifSet1(motifs: Seq[(String, Int)]) =
    new MotifSet(space, ms(motifs).toList).fixMotifs

  def fixedMotifSet2(motifs: Seq[Motif]) =
    new MotifSet(space, motifs.toList).fixMotifs

  def GT(p: Int) = m("GT", p)
  def AT(p: Int) = m("AT", p)
  def AC(p: Int) = m("AC", p)
  def GC(p: Int) = m("GC", p)

  /**
   * Examples of pairs that should not be successors
   * when the expected marker set length is 4.
   */
  def negativeSuccessors4 = Seq(
    (Seq(GT(0), AT(8), AC(2)),
      //GC is < GT in rank so should not appear here
        Seq(GC(0), AT(19), AC(2))),

    (Seq(m("GT", 0), m("AT", 7), m("AT", 14)),
      Seq(m("AT", 0), m("AT", 8), m("AT", 6), m("AT", 7))),

    (Seq(m("GT", 0), m("GT", 2), m("GT", 11), m("AT", 11)),
      Seq(m("GT", 0), m("GT", 2), m("GT", 8), m("AT", 14))),

    (ms(Seq(("AT", 0), ("GT", 15), ("AT", 7), ("AC", 3))),
      ms(Seq(("AT", 0), ("AT", 20), ("AT", 2), ("AC", 3)))),

    (ms(Seq(("GT", 0), ("AT", 4), ("AT", 3), ("AT", 16))),
      ms(Seq(("AT", 0), ("AT", 3), ("AT", 7), ("AT", 9)))),

    (ms(Seq(("AC", 0), ("AT", 2), ("GT", 3), ("GT", 3))),
      ms(Seq(("AC", 0), ("AT", 2), ("GT", 6), ("AT", 7)))),

    (Seq(AT(0), GT(9), AT(7)),
        Seq(GC(0), GT(3), AT(7), GC(6))),

    (Seq(AT(0), AC(17)),
        Seq(GT(0)))

  )

  def positiveSuccessors4 = Seq(
    //AC0 is position-dropped. Then AC6 is introduced by rank on the right.
    (Seq(m("AC", 0), m("AT", 9), m("AT", 5), m("AT", 14)),
      Seq(m("AT", 0), m("AT", 5), m("AC", 6), m("AT", 8))),

    //Here, AT0 was position-dropped and AC0 rank-inserted.
    //AT9 was converted into AT7.
    (Seq(m("AT", 0), m("AT", 9), m("AT", 5), m("AT", 14)),
      Seq(m("AC", 0), m("AT", 7), m("AT", 5), m("AT", 14)))

  )


  def positiveSuccessors3 = Seq(
    (Seq(GT(0), GT(6), GT(20)),
      Seq(GT(0), GT(20), AT(8)))

  )

}