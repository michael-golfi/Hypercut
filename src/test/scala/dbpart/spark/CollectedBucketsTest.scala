package dbpart.spark

import dbpart.bucket.BoundaryBucket
import org.scalatest.{FunSuite, Matchers}
import scala.language.implicitConversions

class CollectedBucketsTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  val k = 4

  implicit class ExtBoundaryBucket(bb: BoundaryBucket) {
    def comparable: (Long, Seq[String], Seq[String], Int) = (bb.id, bb.core.toSeq, bb.boundary.toSeq, bb.k)
  }

  def mkBB(id: Int, core: Array[String], boundary: Array[String]) =
    BoundaryBucket(id, core, boundary, k)

  def mkBB(id: Int, core: String, boundary: String) =
    BoundaryBucket(id, Array(core), Array(boundary), k)

  test("basic") {
    val b1 = mkBB(1, "ACTG", "GGGG")
    val b2 = mkBB(2, "GTTG", "TTTT")

    var cb = CollectedBuckets(b1, 1, true, Array((b2, 1)))
    cb.unified.size should equal(1)
    cb.unified.head.comparable should equal(
      (2, Seq("ACTG", "GTTG", "TTTT"), Seq("GGGG"), 4)
    )

    cb = CollectedBuckets(b1, 1, false, Array((b2, 1)))
    cb.unified.size should equal(1)
    cb.unified.head.comparable should equal(
      b2.comparable
    )

  }

}
