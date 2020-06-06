package hypercut.spark

import hypercut.hash.{MotifSetExtractor, MotifSpace, ReadSplitter}
import org.scalatest.{FunSuite, Matchers}

class CountingTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  val counting = new Counting(spark)

  test("k-mer counting integration test") {
    val data = Seq("ACTGGGTTG", "ACTGTTTTT").toDS()

    val spl = new MotifSetExtractor(MotifSpace.named("all3", 2), 4)

    testSplitter(spl)
  }

  def testSplitter[H](spl: ReadSplitter[H]): Unit = {
    val data = Seq("ACTGGGTTG", "ACTGTTTTT").toDS()
    val verify = List[(String, Long)](
      ("ACTG", 2), ("CTGG", 1), ("TGGG", 1),
      ("CTGT", 1),
      ("GGGT", 1), ("GGTT", 1), ("GTTG", 1),
      ("TGTT", 1), ("GTTT", 1), ("TTTT", 2))

    var counted = counting.countKmers(spl, data, false, false).collect()
    counted should contain theSameElementsAs(verify)

    counted = counting.countKmers(spl, data, false, true).collect()
    counted should contain theSameElementsAs(verify)
  }
}
