package dbpart.bucket

import dbpart.shortread.Read
import org.scalatest.Matchers
import org.scalatest.FunSuite

class BoundaryBucketTest extends FunSuite with Matchers {

  test("sharedOverlaps") {

    import BoundaryBucket._

    //Check that in- and out-openings are correct
    val k = 5
    val ss = Array("ACTGGG", "TTGTTA")
    var post = Array("CCGAT", "GTTAA")
    val postPre = purePrefixes(post.iterator, k).toList
    val postPreSuf = prefixesAndSuffixes(post.iterator, k).toList
    var prior = Array[String]()

    val of = BoundaryBucket(0, Array(), ss, k).overlapFinder
    of.prefixes(Iterator.empty, Iterator.empty) should be(empty)
    of.suffixes(postPreSuf.iterator, postPre.iterator).toSeq should equal(Seq("GTTA"))

    prior = Array("CCGAG", "AACTGAA")
    val priPreSuf = prefixesAndSuffixes(prior.iterator, k).toSeq
    val priSuf = pureSuffixes(prior.iterator, k).toSeq
    of.prefixes(priPreSuf.iterator, priSuf.iterator).toSeq should equal(Seq("ACTG"))

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

    val bnd = Array("TTTA")
    val bkt = BoundaryBucket(1, Array("ACTGGG", "GGGTG", "GTGCA", "GTTT"), Array(), 4)
    val (contigs, updated) = bkt.seizeUnitigs(bnd)
    contigs.map(_.seq) should contain theSameElementsAs(Seq("ACTGGGTGCA"))
    updated.core.toSeq should equal(Seq("GTTT"))
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
    val bkt = BoundaryBucket(1, Array(), Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"), 4)
    bkt.splitSequences should contain theSameElementsAs(List(
      List("ACTGGG", "CTGAA", "GGGA"),
      List("CCCC"),
      List("TTTT")
      ))
  }

  test("seizeUnitigsAndMerge") {
    val core = BoundaryBucket(1, Array(), Array("ACTGGG", "CTGAA", "CCCC", "TTTT", "GGGA"), 4)
    val surrounding = List(BoundaryBucket(2, Array(), Array("TTTA"), 4),
      BoundaryBucket(3, Array(), Array("GCCC"), 4))

    val mrg = BoundaryBucket.seizeUnitigsAndMerge(core, surrounding)
    val unitigs = mrg._1.map(_.seq)
    unitigs.toSeq should contain theSameElementsAs(List("ACTG", "CTGGGA", "CTGAA"))

    val newBkts = mrg._2.map(_._1)
    newBkts.toSeq should contain theSameElementsAs(List(
      Array("TTTT"),
      Array("CCCC")
      ))
  }

  test("removeKmers") {
    val existing = Array("ACTGG", "CCGGT", "GGTTAA")
    val incoming = Array("GCGTGGTTCGTGATTAA") //duplicates GGTT and TTAA
    val r = BoundaryBucket.removeKmers(existing.iterator,
      incoming.flatMap(Read.kmers(_, 4)).toList, 4)
    r should contain theSameElementsAs(List("GCGTGGT", "GTTCGTGATTA"))

    val r2 = BoundaryBucket.removeKmers(existing, incoming, 4)
    r2 should contain theSameElementsAs(List("GCGTGGT", "GTTCGTGATTA"))
  }

}