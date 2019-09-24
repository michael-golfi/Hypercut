package dbpart.spark

import com.google.common.collect.Iterators.MergingIterator
import dbpart.bucket.BoundaryBucket
import org.scalatest.{FunSuite, Matchers}

class IterativeMergeTest extends FunSuite with Matchers {
  import IterativeSerial._

  test("simpleMerge") {
    val k = 5
    val core = Array("ACTGGAGCC")
    val b2 = Array("GGTGGAGG")
    val b2s = Array("GGTGGA", "GGAGG") //without duplicated k-mer
    val b3 = Array("CCCCCC") //no join
    val b4 = Array("AGCCTTG") //join core at end
    val b5 = Array("GTGTGACTG") //join core at front
    val b6 = Array("AATGGA") //join core through end
    val b7 = Array("AGCCAA") //join core through front

    val boundary = Array((2L, b2), (3L, b3), (4L, b4), (5L, b5), (6L, b6), (7L, b7))
    val mb = MergingBuckets(1, true, core, boundary)
    val merged = simpleMerge(k)(mb).map(x => (x._1, x._2.structure, x._3.toSet))

    merged should contain theSameElementsAs(Seq(
      (1L, (1, core.toSet ++ b2s ++ b4 ++ b5 ++ b6 ++ b7, k), Set(1L, 2L, 4L, 5L, 6L, 7L)),
      (3L, (3, b3.toSet, k), Set(3L))
    ))
  }

}
