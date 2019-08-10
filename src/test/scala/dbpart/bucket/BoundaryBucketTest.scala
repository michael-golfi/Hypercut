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

    var inSet = Set("CCGA", "CGAG", "CTGA", "CGAT", "GTTA")
    //cannot contain initial k-1-mers in prior
    BoundaryBucket.sharedOverlapsFrom(prior.iterator, inSet, k).toList should
      contain theSameElementsAs(Seq("CGAG", "CTGA"))

    //Cannot contain final k-1-mers in post
    BoundaryBucket.sharedOverlapsTo(post.iterator, inSet, k).toList should
      contain theSameElementsAs(Seq("CCGA", "GTTA"))
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
    bkt.removeSequences(Seq("ACTG")).kmers.toList should
      contain theSameElementsAs(Seq("CTGG", "TGGG", "GTGC", "TGCA"))
  }

  test("unifyParts") {

    val testData = List((List(1L, 2L, 3L), List("a")),
      (List(1L, 4L), List("b")),
      (List(5L, 6L), List("c")))
    val un = BoundaryBucket.unifyParts(testData)

    //Should merge 3 parts into 2
    un.map(x => (x._2.toSet, x._3.toSet)) should contain theSameElementsAs(
      List(
        (Set("a", "b"), Set(1L, 2L, 3L, 4L)),
        (Set("c"), Set(5L, 6L))
        ))

    //Adding this part should force all the parts to merge into one
    val testData2 = (List(5L, 4L), List("d")) :: testData
    val un2 = BoundaryBucket.unifyParts(testData2)
    un2.map(x => (x._2.toSet, x._3.toSet)) should contain theSameElementsAs(
      List(
        (Set("a", "b", "c", "d"), Set[Long](1, 2, 3, 4, 5, 6))
        ))
  }
}