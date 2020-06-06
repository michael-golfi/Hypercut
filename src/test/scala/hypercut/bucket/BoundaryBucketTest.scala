package hypercut.bucket

import hypercut.shortread.Read
import org.scalatest.Matchers
import org.scalatest.FunSuite

class BoundaryBucketTest extends FunSuite with Matchers {
  import Util._

  test("sharedOverlaps") {

    //Check that in- and out-openings are correct
    val k = 5
    val ss = Array("ACTGGG", "TTGTTA")
    var post = Array("CCGAT", "GTTAA")
    val postPre = purePrefixes(post.iterator, k).toList
    val postPreSuf = prefixesAndSuffixes(post.iterator, k).toList
    var prior = Array[String]()

    val of = BoundaryBucket(0, Array(), ss).ops(k).overlapFinder
    prior = Array("CCGAG", "AACTGAA")

    val all = prior ++ post

    of.find(prefixesAndSuffixes(all.iterator, k),
      purePrefixes(all.iterator, k),
      pureSuffixes(all.iterator, k)).toList should equal(Seq("ACTG", "GTTA"))
  }

  test("containedUnitigs") {
    // Check that unitigs entirely contained in buckets (not touching boundary)
    // are identified
  }

  test("seizeUnitigs") {
    //Check that unitigs contained entirely in the bucket
    //can be obtained, and that they do not remain in the bucket
    //afterward

    val k = 4
    val bnd = Array("TTTA")
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GGGTG", "GTGCA", "GTTT"), bnd)
    val (contigs, updated) = bkt.ops(k).seizeUnitigs
    contigs.map(_.seq) should contain theSameElementsAs(Seq("ACTGGGTGCA"))
    updated.core.toSeq should equal(Seq("GTTT"))
  }

  test("removeSequences") {
    val k = 4
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GTGCA"), Array())
    bkt.ops(k).removeSequences(Seq("ACTG")).ops(k).kmers.toList should
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
    //TODO test boundary flag as well
    val k = 4
    val bkt = BoundaryBucket(1, Array(), Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"))
    bkt.ops(k).splitSequences.map(_.map(_._1).sorted) should contain theSameElementsAs(List(
      List("ACTGGG", "CTGAA", "GGGA").sorted,
      List("CCCC"),
      List("TTTT")
      ))
  }

  test("seizeUnitigsAndMerge") {
    val bnd = Array("TTTA", "GCCC")
    val k = 4
    val core = BoundaryBucket(1, Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"), bnd)

    val mrg = core.ops(k).seizeUnitigsAndSplit
    val unitigs = mrg._1.map(_.seq)
    unitigs.toSeq should contain theSameElementsAs(List("ACTG", "CTGGGA", "CTGAA"))

    val newBkts = mrg._2.map(_.core)
    newBkts.toSeq should contain theSameElementsAs(List(
      Array("TTTT"),
      Array("CCCC")
      ))
  }

  test("removeKmers") {
    val existing = Array("ACTGG", "CCGGT", "GGTTAA")
    val incoming = Array("GCGTGGTTCGTGATTAA") //duplicates GGTT and TTAA
    val k = 4
    val r = BoundaryBucket.removeKmers(existing.iterator,
      incoming.flatMap(Read.kmers(_, 4)).toList, k)
    r should contain theSameElementsAs(List("GCGTGGT", "GTTCGTGATTA"))

    val r2 = BoundaryBucket.removeKmers(existing, incoming, k)
    r2 should contain theSameElementsAs(List("GCGTGGT", "GTTCGTGATTA"))
  }

}