package dbpart.bucket

import org.scalatest.Matchers
import org.scalatest.FunSuite
import miniasm.genome.util.DNAHelpers
import dbpart.shortread.Read

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

  def withAbund(seqs: Iterable[String]) = seqs.map(s => (s, 1.toShort))

  test("large") {
    val k = 51

    val n = 100
    var seqs = (0 until n).map(x => DNAHelpers.randomSequence(100))
    val kmers = seqs.flatMap(s => Read.kmers(s, k))

    var b = SimpleCountingBucket.empty(k)
    b = b.insertBulkSegments(withAbund(seqs))
    b.kmers.sorted should equal(kmers.distinct.sorted)
    b.sequences.size should equal(n)

    seqs = (0 until n).map(x => DNAHelpers.randomSequence(k))
    b = SimpleCountingBucket.empty(k)
    b = b.insertBulkSegments(withAbund(seqs))
    //Sequences should merge correctly on insertion
    val postExt = seqs.map(s => DNAHelpers.extendSeq(s, 1))
    b = b.insertBulkSegments(withAbund(postExt))
    b.sequences.size should be <= n
    val preExt = seqs.map(s => "A" + s)
    b = b.insertBulkSegments(withAbund(preExt))
    b.sequences.size should be <= n

    b = b.insertBulkSegments(withAbund(seqs))
    b.sequences.size should be <= n

    val allKmers = (preExt ++ postExt).flatMap(r => Read.kmers(r, k)).distinct
    b.kmers.sorted should equal(allKmers.sorted)
  }

  test("merging") {
    val k = 5
    val seqs = Seq("ACTGG", "TGGAA")
    var b = SimpleCountingBucket.empty(k)
    b = b.insertBulkSegments(withAbund(seqs))
    b = b.insertBulkSegments(withAbund(Seq("CTGGA")))
    b.sequences should equal(Seq("ACTGGAA"))
  }
}