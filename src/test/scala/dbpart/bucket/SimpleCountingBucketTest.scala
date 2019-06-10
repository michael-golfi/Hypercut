package dbpart.bucket

import org.scalatest.Matchers
import org.scalatest.FunSuite

class SimpleCountingBucketTest extends FunSuite with Matchers {
  val k = 5

  test("simple") {
    var b = SimpleCountingBucket.empty(k)

    b = b.insertBulk(Seq("ACTGG", "CTGGA")).getOrElse(b)
    b.numKmers should equal(2)
    b.sequences.size should equal(1)
    b.abundances should equal(Array(Array(1,1)))

    b = b.insertBulk(Seq("CTGGA")).getOrElse(b)
    b.numKmers should equal(2)
    b.sequences.size should equal(1)
    b.abundances should equal(Array(Array(1,2)))

    b = b.insertBulk(Seq("TTTTT")).getOrElse(b)
    b.numKmers should equal(3)
    b.sequences.size should equal(2)
    b.abundances should equal(Array(Array(1,2), Array(1)))
  }

  test("segments") {
    var b = SimpleCountingBucket.empty(k)
    val segAbund = Seq(("ACTGGTT", 2.toShort), ("TTTATT", 1.toShort))
    b = b.insertBulkSegments(segAbund)
    b.sequences.size should equal(2)
    b.abundances should equal(Array(Array(2, 2, 2), Array(1, 1)))

  }
}