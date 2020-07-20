package hypercut.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class IterativeMergeTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

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
//    val mb = MergingBuckets(1, true, core, boundary)

    //TODO update tests
//    val merged = simpleMerge(k)(mb).map(x => (x._1, x._2.structure, x._3.toSet))
//
//    merged should contain theSameElementsAs(Seq(
//      (1L, (1, core.toSet ++ b2s ++ b4 ++ b5 ++ b6 ++ b7, k), Set(1L, 2L, 4L, 5L, 6L, 7L)),
//      (3L, (3, b3.toSet, k), Set(3L))
//    ))
  }

  def mkEdges(es: (Long,Long)*): Seq[(Long, Long)] =
    es.toSeq

  def mkRows(es: (Long, Long)*) =
    es.map(e => Row(e._1, e._2))

  test("IterativeMerge.translateAndNormalizeEdges") {
    var edges = Seq((1L, 2L), (1L, 2L)).toDF("src", "dst")
    var translation = Seq((1L, 1L),(2L,2L)).toDF("src", "dst")
    val iterativemerge = new IterativeMerge(spark, false, None, 3, "output")

    var translatedEdges = iterativemerge.translateAndNormalizeEdges(edges, translation) //Remove duplicates in edges
    assert(translatedEdges.collect() === mkRows((1,2)))

    translation = Seq((3L, 4L)).toDF("src", "dst")
    translatedEdges = iterativemerge.translateAndNormalizeEdges(edges, translation) //Remove all edges if translation map does not contain any of the edges
    assert(translatedEdges.count() == 0)

    edges = Seq((1L, 1L)).toDF("src", "dst")
    translation = Seq((1L, 1L)).toDF("src", "dst")
    translatedEdges = iterativemerge.translateAndNormalizeEdges(edges, translation) //Self edges should disappear
    assert(translatedEdges.count() == 0)

    edges = mkEdges((2, 1)).toDF("src", "dst")
    translation = mkEdges((1, 3), (2, 2)).toDF("src", "dst")
    translatedEdges = iterativemerge.translateAndNormalizeEdges(edges, translation) //Edge value should translate to some other value as per the translation table
    assert(translatedEdges.collect() === mkRows((2,3)))

    edges = mkEdges((2, 1)).toDF("src", "dst")
    translation = mkEdges((1, 1), (2, 2)).toDF("src", "dst")
    translatedEdges = iterativemerge.translateAndNormalizeEdges(edges, translation) //The order of values in the edge should be changed to ascending order
    assert(translatedEdges.collect() === mkRows((1, 2)))
  }
}
