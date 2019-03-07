package dbpart

import org.scalatest.Matchers
import org.scalatest.FunSuite
import org.scalactic.source.Position.apply
import scala.collection.Seq

class KmerSpaceTest extends FunSuite with Matchers {
  import Testing._

  def mkSpace(mss: Iterable[MarkerSet]) = {
    val kms = new KmerSpace
    for (ms <- mss) {
      kms.add(ms)
    }
    kms
  }

  test("basic and constrained") {
    val head = AT(3)
    val m2 = AC(2)
    val m3a = space.get("GT", 5, true)
    val m3b = space.get("GC", 5, true)

    val ms1 = fixedMarkerSet2(Seq(head, m2, m3a))
    val ms2 = fixedMarkerSet2(Seq(head, m2, m3b))
    val mss = Seq(ms1, ms2)

    val kms = mkSpace(mss)
    kms.size should equal(2)

    val ss = kms.getSubspace(head).get
    ss.size should equal(2)

    val const = kms.asConstrained

    const.toSeq should equal(mss)
    const.heads.toSeq should equal(Seq(head))
    const.headPosition should equal(0)
    const.canStepForward should be(true)
    const.size should equal(2)
    const.breadth should equal(1)
    const.currentLeaves should be(empty)

    const.dropAndAddOffset.heads.toSeq should equal(Seq(AC(5)))
    const.stepForward(3).heads.toSeq should equal(Seq(m2))

    val dz = const.stepForward(head).constrainMarker(m2).dropAndAddOffset
    dz.heads.toSeq should contain theSameElementsAs(Seq(space.get("GT", 7, true), space.get("GC", 5, true)))
    dz.toSeq should equal(mss)

    val s1 = const.stepForward(head)
    s1.canStepForward should be(true)
    s1.heads.toSeq should equal(Seq(m2))
    s1.headPosition should equal(1)
    s1.size should equal(2)
    s1.breadth should equal(1)
    s1.currentLeaves should be(empty)

    val zeroHeads = Seq(space.get("GT", 0), space.get("GC", 0))
    s1.dropAndSetToZero.heads.toSeq should contain theSameElementsAs(zeroHeads)

    val s2 = s1.stepForward(m2)
    s2.canStepForward should be(true)
    s2.heads.toSeq should contain theSameElementsAs(Seq(m3a, m3b))
    s2.headPosition should equal(2)
    s2.size should equal(2)
    s2.breadth should equal(2)
    s2.currentLeaves should be(empty)

    s2.setHeadsToZero.heads.toSeq should contain theSameElementsAs(zeroHeads)

    val s3 = s2.stepForward(m3a)
    s3.canStepForward should be(false)
    s3.heads.toSeq should equal(Seq())
    s3.headPosition should equal(3)
    s3.size should equal(0)
    s3.breadth should equal(0)
    s3.currentLeaves should equal(Seq(ms1))
  }

  test("lengths") {
    val head = AT(3)
    val m2 = AC(2)
    val m3a = space.get("GT", 5, true)
    val m3b = space.get("GC", 3, true)
    val m4 = AT(2)

    val ms4 = fixedMarkerSet2(Seq(head, m2, m3a, m4))
    val ms3a = fixedMarkerSet2(Seq(head, m2, m3a))
      val ms3b = fixedMarkerSet2(Seq(head, m2, m3b))
    val mss = Set(ms3a, ms3b, ms4)

    val kms = mkSpace(mss)

    val cons = kms.asConstrained
    cons.atMinLength(1).toSet should equal(mss)
    cons.atMinLength(2).toSet should equal(mss)
    cons.atMinLength(3).toSet should equal(mss)
    cons.atMinLength(4).toSet should equal(Set(ms4))
    cons.atMinLength(5).toSet should equal(Set())

    cons.atLength(3).toSet should equal(Set(ms3a, ms3b))
    cons.atLength(3).stepForward(head).toSet should equal(Set(ms3a, ms3b))
    cons.atLength(3).dropAndAddOffset.toSet should equal(Set(ms3a, ms3b))
    cons.atLength(4).toSet should equal(Set(ms4))

    cons.atMaxLength(3).toSet should equal(Set(ms3a, ms3b))
    cons.atMaxLength(4).toSet should equal(mss)

    cons.currentLeaves should be(empty)
    val c2 = cons.stepForward(head).stepForward(m2).stepForward(m3a)
    c2.currentLeaves should equal(Seq(ms3a))
    c2.atLength(1).currentLeaves should be(empty)
    c2.atLength(2).currentLeaves should be(empty)
    c2.atMinLength(2).currentLeaves should equal(Seq(ms3a))
    c2.atLength(3).currentLeaves should equal(Seq(ms3a))
    c2.atLength(4).currentLeaves should be(empty)
    c2.atMaxLength(4).currentLeaves should equal(Seq(ms3a))

    val c3 = c2.stepForward(m4)
    c3.currentLeaves should equal(Seq(ms4))
    c3.atLength(4).currentLeaves should equal(Seq(ms4))
    c3.atLength(5).currentLeaves should be(empty)
  }

  def asFixed(edges: Iterable[(Seq[Marker], Seq[Marker])]) =
    edges.map(x =>
      ((new MarkerSet(space, x._1.toList).fixMarkers,
          new MarkerSet(space, x._2.toList).fixMarkers)))

  def makeAllEdges(markers: Iterable[MarkerSet], n: Int) = {
    val kms = mkSpace(markers)
    kms.completeEdges(space, n).toSet
  }

  test("edges negative") {
    val negAsMs = asFixed(negativeSuccessors4)

    val all = makeAllEdges(negAsMs.flatMap(x => Seq(x._1, x._2)), 4)
    for ((a, b) <- negAsMs) {
      all should not contain((a,b))
    }
  }

  test("edges positive") {
    val posAsMs = asFixed(positiveSuccessors4)

    val all = makeAllEdges(posAsMs.flatMap(x => Seq(x._1, x._2)), 4)
    for ((a, b) <- posAsMs) {
      all should contain((a,b))
    }

    val pos3AsMs = asFixed(positiveSuccessors3)
    val all3 = makeAllEdges(pos3AsMs.flatMap(x => Seq(x._1, x._2)), 3)
    for ((a, b) <- pos3AsMs) {
      all3 should contain((a,b))
    }
  }

  test("edges distinct") {
    val negAsMs = negativeSuccessors4.map(x =>
      ((new MarkerSet(space, x._1.toList).fixMarkers,
          new MarkerSet(space, x._2.toList).fixMarkers)))
    val posAsMs = positiveSuccessors4.map(x =>
      ((new MarkerSet(space, x._1.toList).fixMarkers,
          new MarkerSet(space, x._2.toList).fixMarkers)))

    val allMarkerSets = (negAsMs ++ posAsMs).
      flatMap(x => Seq(x._1, x._2))
    val kms = mkSpace(allMarkerSets)
    val es = kms.completeEdges(space, 4)

    var seen = Set[(MarkerSet, MarkerSet)]()
    for (n <- es) {
      seen should not contain(n)
      seen += n
    }
  }
}