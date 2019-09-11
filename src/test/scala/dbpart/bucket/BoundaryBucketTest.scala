package dbpart.bucket

import dbpart.Testing
import org.scalatest.Matchers
import org.scalatest.FunSuite

class BoundaryBucketTest extends FunSuite with Matchers {
  import Testing._

  def it[T](d: Iterable[T]) = d.iterator

  test("sharedOverlaps") {

    //Check that in- and out-openings are correct
    val k = 5
    val ss = Array("ACTGGG", "TTGTTA")
    var post = Array("CCGAT", "GTTAA")
    var prior = Array[String]()
    BoundaryBucket.sharedOverlapsThroughPrior(it(prior), it(ss), k).toSeq should equal (Seq())
    BoundaryBucket.sharedOverlapsThroughPrior(it(ss), it(post), k).toSeq should equal (Seq("GTTA"))

    prior = Array("CCGAG", "AACTGAA")
    BoundaryBucket.sharedOverlapsThroughPrior(it(prior), it(ss), k).toSeq should equal(Seq("ACTG"))

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

    val bnd = Array("TTTA")
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GGGTG", "GTGCA", "GTTT"), 4)
    val (contigs, updated) = bkt.seizeUnitigs(bnd)
    contigs.map(_.seq) should contain theSameElementsAs(Seq("ACTGGGTGCA"))
    updated.core.toSeq should equal(Seq("GTTT"))
  }

  test("removeSequences") {
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GTGCA"), 4)
    bkt.removeSequences(Seq("ACTG")).kmers.toList should
      contain theSameElementsAs(Seq("CTGG", "TGGG", "GTGC", "TGCA"))
  }

  test("unifyParts") {

    val testData = List((List(1L, 2L, 3L), List("a")),
      (List(1L, 4L), List("b")),
      (List(5L, 6L), List("c")))
    val un = BoundaryBucket.unifyParts(testData)

    //Should merge 3 parts into 2
    un.map(x => (x._1.toSet, x._2.toSet)) should contain theSameElementsAs(
      List(
        (Set("a", "b"), Set(1L, 2L, 3L, 4L)),
        (Set("c"), Set(5L, 6L))
        ))

    //Adding this part should force all the parts to merge into one
    val testData2 = (List(5L, 4L), List("d")) :: testData
    val un2 = BoundaryBucket.unifyParts(testData2)
    un2.map(x => (x._1.toSet, x._2.toSet)) should contain theSameElementsAs(
      List(
        (Set("a", "b", "c", "d"), Set[Long](1, 2, 3, 4, 5, 6))
        ))
  }

  test("splitSequences") {
    val bkt = BoundaryBucket(1, Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"), 4)
    bkt.splitSequences should contain theSameElementsAs(List(
      List("ACTGGG", "CTGAA", "GGGA"),
      List("CCCC"),
      List("TTTT")
      ))
  }

  test("seizeUnitigsAndMerge") {
    val core = BoundaryBucket(1, Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"), 4)
    val boundary = List(BoundaryBucket(2, Array("TTTA"), 4),
      BoundaryBucket(3, Array("GCCC"), 4))

    val mrg = BoundaryBucket.seizeUnitigsAndMerge(core, boundary)
    val unitigs = mrg._1.map(_.seq)
    unitigs.toSeq should contain theSameElementsAs(List("ACTG", "CTGGGA", "CTGAA"))

    val newBkts = mrg._2.map(_._1)
    newBkts.toSeq should contain theSameElementsAs(List(
      Array("TTTT"),
      Array("CCCC")
      ))
  }

}