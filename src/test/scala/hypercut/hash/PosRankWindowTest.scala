package hypercut.hash

import hypercut.Testing
import org.scalatest.{FunSuite, Matchers}

class PosRankWindowTest extends FunSuite with Matchers {
  import Testing._

  test("position") {
    val test = ms(Seq(("AC", 3), ("AC", 5), ("GT", 10)))
    val prl = PosRankWindow(test)
    prl should contain theSameElementsAs(test)

    prl.dropUntilPosition(2, space)
    prl should contain theSameElementsAs(test)
    //Minimum permitted start offset means that (AC,3) cannot be 
    //parsed after position 2.
    prl.dropUntilPosition(3, space)
    
    prl should contain theSameElementsAs(test.drop(1))
    prl.dropUntilPosition(4, space)
    prl should contain theSameElementsAs(test.drop(1))
    prl.dropUntilPosition(6, space)
    prl should contain theSameElementsAs(test.drop(2))
    prl.dropUntilPosition(11, space)
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
    var test = ms(Seq(("AC", 3), ("AC", 5), ("CTT", 6), ("GT", 15)))
    var prl = PosRankWindow(test)
    assert(prl.takeByRank(3) === Seq(test(0), test(1), test(3)))

    test = ms(Seq(("TTA", 3), ("AC", 5), ("AT", 6), ("GT", 15)))
    prl = PosRankWindow(test)
    assert(prl.takeByRank(3) === Seq(test(0), test(2), test(3)))

    test = ms(Seq(("AT", 1), ("TTA", 2), ("AC", 4), ("AT", 6), ("GT", 15)))
    prl = PosRankWindow(test)
    assert(prl.takeByRank(1) === Seq(test(1)))
    assert(prl.takeByRank(3) === Seq(test(1), test(3), test(4)))

    test = ms(Seq(("TTA", 5), ("AC", 7)))
    prl = PosRankWindow(test)
    assert(prl.takeByRank(1) === Seq(test(0)))
    assert(prl.takeByRank(2) === Seq(test(0)))
   }

}