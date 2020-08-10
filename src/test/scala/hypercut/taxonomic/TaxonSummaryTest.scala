package hypercut.taxonomic

import org.scalatest.{FunSuite, Matchers}
import scala.collection.mutable.Seq
import scala.collection.mutable.Map

class TaxonSummaryTest extends FunSuite with Matchers {

  test("basic") {
    TaxonSummary.fromClassifiedKmers(Iterator(1,1,3,1,1,5), 1) should equal(
      TaxonSummary(1, Seq(1, 3, 1, 5), Seq(2, 1, 2, 1))
    )
  }

  def append(t1: TaxonSummary, t2: TaxonSummary) = {
    TaxonSummary.concatenate(Seq(t1, t2))
  }

  test("concatenate") {
    append(TaxonSummary(1, Seq(1, 3), Seq(2, 1)),
      TaxonSummary(2, Seq(3, 2), Seq(1, 1))) should equal(
        TaxonSummary(1,  Seq(1, 3, 2), Seq(2, 2, 1)))

    append(TaxonSummary(1, Seq(1, 3), Seq(2, 1)),
      TaxonSummary(2, Seq(4, 2), Seq(1, 1))) should equal(
      TaxonSummary(1,  Seq(1, 3, 4, 2), Seq(2, 1, 1, 1)))

    append(TaxonSummary(1, Seq(1), Seq(1)),
      TaxonSummary(2, Seq(1), Seq(1))) should equal(
      TaxonSummary(1,  Seq(1), Seq(2)))
  }

  test("mergeHits") {
    val testVals = List(
      TaxonSummary(1, Seq(1, 3), Seq(1, 1)),
      TaxonSummary(2, Seq(1, 2), Seq(1, 1)),
      TaxonSummary(3, Seq(1, 4), Seq(1, 1))
    )
    TaxonSummary.mergeHitCounts(testVals) should equal (Map(1 -> 3, 2 -> 1, 3 -> 1, 4 -> 1))

    val testVals2 = List(
      TaxonSummary(1, Seq(1), Seq(1)),
      TaxonSummary(2, Seq(1), Seq(1))
    )
    TaxonSummary.mergeHitCounts(testVals2) should equal (Map(1 -> 2))
  }
}
