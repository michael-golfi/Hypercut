package dbpart

import scala.collection.Seq

import org.scalactic.source.Position.apply
import org.scalatest.FunSuite
import org.scalatest.Matchers

class MarkerSetTest extends FunSuite with Matchers {
  import Testing._

  def neg(ms1: Seq[Marker], ms2: Seq[Marker], n: Int) = {
    println(s"Test: $ms1 < $ms2 == false")
    new MarkerSet(space, ms1).fixMarkers.precedes(new MarkerSet(space, ms2).fixMarkers, n) should equal(false)
  }

  def neg4(ms1: Seq[Marker], ms2: Seq[Marker]) = neg(ms1, ms2, 4)
  def neg5(ms1: Seq[Marker], ms2: Seq[Marker]) = neg(ms1, ms2, 5)

  def pos(ms1: Seq[Marker], ms2: Seq[Marker], n: Int) = {
    println(s"Test: $ms1 < $ms2 == true")
    new MarkerSet(space, ms1).fixMarkers.precedes(new MarkerSet(space, ms2).fixMarkers, n) should equal(true)
  }

  def pos3(ms1:Seq[Marker], ms2: Seq[Marker]) = pos(ms1, ms2, 3)
  def pos4(ms1: Seq[Marker], ms2: Seq[Marker]) = pos(ms1, ms2, 4)
  def pos5(ms1: Seq[Marker], ms2: Seq[Marker]) = pos(ms1, ms2, 5)


  test("precedes should fail") {
    for ((a, b) <- negativeSuccessors4) {
      neg4(a, b)
    }

     //Here, (GT, 2) should not disappear from the left hand side
   neg5(
     ms(Seq(("AT",0), ("AT",3), ("GT",4), ("GT",13), ("GT",2))),
     ms(Seq(("AT",0), ("GT",4), ("GT",13), ("AT",3), ("GT",3))))
  }

  test("precedes should succeed") {
    for ((a, b) <- positiveSuccessors4) {
      pos4(a, b)
    }

    for ((a, b) <- positiveSuccessors3) {
      pos3(a,b)
    }
  }
}