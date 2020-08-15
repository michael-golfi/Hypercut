package hypercut.hash

import hypercut.Testing
import org.scalatest.{FunSuite, Matchers}

//Most tests in this file are sensitive to the order of markers in the all2 space
class PosRankWindowTest extends FunSuite with Matchers {
  import Testing._

  test("position") {
    val test = ms(Seq(("AC", 3), ("AC", 5), ("GT", 10)))
    val prl = PosRankWindow(test)
    prl should contain theSameElementsAs(test)

    prl.dropUntilPosition(2)
    prl should contain theSameElementsAs(test)

    prl.dropUntilPosition(3)
    prl should contain theSameElementsAs(test)
    prl.dropUntilPosition(4)
    prl should contain theSameElementsAs(test.drop(1))
    prl.dropUntilPosition(6)
    prl should contain theSameElementsAs(test.drop(2))
    prl.dropUntilPosition(11)
    prl should contain theSameElementsAs(Seq())

    assert(prl.end === End())
  }

   test("rank") {
    val test = ms(Seq(("AC", 3), ("AC", 5), ("AT", 10), ("GT", 15)))
    val prl = PosRankWindow(test)
    prl should contain theSameElementsAs(test)

    assert(prl.takeByRank(2) === Seq(test(0), test(2)))
    assert(prl.takeByRank(3) === Seq(test(0), test(1), test(2)))
    assert(prl.takeByRank(4) === Seq(test(0), test(1), test(2), test(3)))
  }

   test("insert") {
    val test = ms(Seq(("AC", 3), ("AC", 5), ("AT", 10), ("GT", 15)))
    val prl = PosRankWindow(test)
    val app = m("AC", 17)
    prl :+= app
    assert(prl.takeByRank(4) === Seq(test(0), test(1), test(2), app))
    prl should contain theSameElementsAs(test :+ app)
   }

   test("overlap resolution") {
    var test = ms(Seq(("AC", 3), ("AC", 5), ("CT", 6), ("GT", 15)))
    var prl = PosRankWindow(test)
    assert(prl.takeByRank(3) === Seq(test(0), test(2), test(3)))

    test = ms(Seq(("TTA", 3), ("AC", 5), ("AT", 6), ("GT", 15)))
    prl = PosRankWindow(test)
    assert(prl.takeByRank(3) === Seq(test(0), test(2), test(3)))

    test = ms(Seq(("TT", 1), ("AT", 2), ("AC", 4), ("AT", 6), ("GT", 15)))
    prl = PosRankWindow(test)
    assert(prl.takeByRank(1) === Seq(test(1)))
    assert(prl.takeByRank(3) === Seq(test(1), test(2), test(3)))
   }

}