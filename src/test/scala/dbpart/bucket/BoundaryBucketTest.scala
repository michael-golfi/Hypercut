package dbpart.bucket

import dbpart.Testing
import org.scalatest.Matchers
import org.scalatest.FunSuite

class BoundaryBucketTest extends FunSuite with Matchers {
  import Testing._

  test("sharedOverlaps") {

    //Check that in- and out-openings are correct
    val k = 5
    val ss = Array("ACTGGG", "TTGTTA")
    var post = Array("CCGAT", "GTTAA")
    var prior = Array[String]()
    BoundaryBucket.sharedOverlaps(prior, ss, k) should equal (Seq())
    BoundaryBucket.sharedOverlaps(ss, post, k) should equal (Seq("GTTA"))

    prior = Array("CCGAG", "AACTGAA")
    BoundaryBucket.sharedOverlaps(prior, ss, k) should equal(Seq("ACTG"))

//    val post = Array("CCGAT", "GTTAA")
  }

  test("containedUnitigs") {
    // Check that unitigs entirely contained in buckets (not touching boundary)
    // are identified
  }

  test("seizeUnitigs") {
    //Check that unitigs contained entirely in the bucket
    //can be obtained, and that they do not remain in the bucket
    //afterward
  }
}