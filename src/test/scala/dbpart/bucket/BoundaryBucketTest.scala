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
    BoundaryBucket.sharedOverlapsThroughPrior(prior, ss, k).toSeq should equal (Seq())
    BoundaryBucket.sharedOverlapsThroughPrior(ss, post, k).toSeq should equal (Seq("GTTA"))

    prior = Array("CCGAG", "AACTGAA")
    BoundaryBucket.sharedOverlapsThroughPrior(prior, ss, k).toSeq should equal(Seq("ACTG"))

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
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GTGCA"), Array(), 4)
    val (contigs, updated) = bkt.seizeUnitigs
    contigs.map(_.seq) should contain theSameElementsAs(Seq("ACTGGG", "GTGCA"))
    updated.core.toSeq should equal(Seq())
  }

  test("removeSequences") {
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GTGCA"), Array(), 4)
    bkt.removeSequences(Seq("ACTG")).kmers.toList should contain theSameElementsAs(Seq("CTGG", "TGGG", "GTGC", "TGCA"))
  }
}