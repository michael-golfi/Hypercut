package hypercut.spark

import hypercut.bucket.BoundaryBucket
import org.scalatest.{FunSuite, Matchers}
import scala.language.implicitConversions

class CollectedBucketsTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  val k = 4

  implicit class ExtBoundaryBucket(bb: BoundaryBucket) {
    def comparable: (Long, Seq[String], Seq[String]) = (bb.id, bb.core.toList, bb.boundary.toList)
  }

  def mkBB(id: Int, core: Array[String], boundary: Array[String]) =
    BoundaryBucket(id, core, boundary)

  def mkBB(id: Int, core: String, boundary: String) =
    BoundaryBucket(id, Array(core), Array(boundary))

  test("basic") {
    val b1 = mkBB(1, "ACTG", "GGGG")
    val b2 = mkBB(2, "GTTG", "TTTT")

    var cb = CollectedBuckets(b1, 1, true, Array((b2, 1)))
    cb.unified(k).size should equal(1)
    cb.unified(k).head.comparable should equal(
      (2, List("ACTG", "GTTG", "TTTT"), List("GGGG"))
    )

    cb = CollectedBuckets(b1, 1, false, Array((b2, 1)))
    cb.unified(k).size should equal(1)
    cb.unified(k).head.comparable should equal(
      b2.comparable
    )
  }
}
