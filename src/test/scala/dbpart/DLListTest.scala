package dbpart

import org.scalatest.Matchers
import org.scalatest.FunSuite

class DLListTest extends FunSuite with Matchers {
  import Testing._

  test("position") {
    val test = ms(Seq(("AC", 3), ("AC", 4), ("GT", 10)))
    val prl = PosRankList(test)
    prl should contain theSameElementsAs(test)

    prl.dropUntilPosition(3)
    prl should contain theSameElementsAs(test)
    prl.dropUntilPosition(4)
    prl should contain theSameElementsAs(test.drop(1))
    prl.dropUntilPosition(5)
    prl should contain theSameElementsAs(test.drop(2))
    prl.dropUntilPosition(11)
    prl should contain theSameElementsAs(Seq())
  }

   test("rank") {
    val test = ms(Seq(("AC", 3), ("AC", 4), ("AT", 10), ("GT", 15)))
    val prl = PosRankList(test)
    prl should contain theSameElementsAs(test)

    assert(prl.takeByRank(2) === Seq(test(2), test(0)))
    assert(prl.takeByRank(3) === Seq(test(2), test(0), test(1)))
    assert(prl.takeByRank(4) === Seq(test(2), test(0), test(1), test(3)))
  }

   test("insert") {
    val test = ms(Seq(("AC", 3), ("AC", 4), ("AT", 10), ("GT", 15)))
    val prl = PosRankList(test)
    val app = m("AC", 5)
    prl :+= app
    assert(prl.takeByRank(4) === Seq(test(2), test(0), test(1), app))
    prl should contain theSameElementsAs(test :+ app)

   }
}