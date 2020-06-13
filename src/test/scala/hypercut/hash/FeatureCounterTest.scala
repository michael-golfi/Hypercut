package hypercut.hash

import org.scalatest.{FunSuite, Matchers}

class FeatureCounterTest extends FunSuite with Matchers {

  test("basic") {
    val space = MotifSpace.using(MotifSpace.all3mers, 2, "default")
    val reads = Seq("ACTGTT", "TGGTTCCA")
    val counter = FeatureCounter(space)
    val scanner = new FeatureScanner(space)

    for (r <- reads) {
      scanner.scanRead(counter, r)
    }
    counter.motifsWithCounts(space).toSeq.filter(_._2 > 0) should contain theSameElementsAs(
      List[(String, Long)](
        ("ACT", 1), ("CTG", 1), ("TGT", 1),
        ("GTT", 2), ("TGG", 1), ("GGT", 1),
        ("TTC", 1), ("TCC", 1), ("CCA", 1)
      ))

  }

}
