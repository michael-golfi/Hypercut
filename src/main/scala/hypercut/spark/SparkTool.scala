package hypercut.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

import hypercut._
import hypercut.hash.MarkerSetExtractor
import org.rogach.scallop.ScallopOption

import hypercut.bucket.CountingSeqBucket._
import hypercut.hash.MarkerSpace

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    val conf = new SparkConf
    conf
  }

  lazy val spark =
    SparkSession.builder().appName(appName).
      master("spark://localhost:7077").config(conf).getOrCreate()

  lazy val routines = new Routines(spark)
}

class HCSparkConf(args: Array[String], spark: SparkSession) extends CoreConf(args) {
  import spark.sqlContext.implicits._

  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson (Spark version)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  def routines = new Routines(spark)

  val checkpoint = opt[String](required = false, default = Some("/ext/scratch/spark/checkpoint"),
      descr = "Path to checkpoint directory")

  def getSpace(input: String): MarkerSpace = {
    sample.toOption match {
      case Some(amount) => routines.createSampledSpace(input, amount, defaultSpace)
      case None => defaultSpace
    }
  }

  val buckets = new Subcommand("buckets") {
    val location = opt[String](required = true, descr = "Path to location where buckets and edges are stored (parquet)")

    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true, descr = "Path to input data files")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
         val ext = new MarkerSetExtractor(getSpace(input()), k())
         val minAbundance = None
         if (edges()) {
           routines.graphFromReads(input(), ext, location.toOption)
         } else {
           routines.bucketsOnly(input(), ext, location())
         }
      }
    }
    addSubcommand(build)

    val top = new Subcommand("top") with RunnableCommand {
      val n = opt[Int](required = false, default = Some(10),
          descr = "Number of buckets to print")
      val amount = opt[Int](required = false, default = Some(10),
          descr = "Amount of sequence to print")

      def run() {
       val buckets = routines.loadBuckets(location())
       routines.showBuckets(buckets.map(_._2),
           n(), amount())
      }
    }
    addSubcommand(top)

    val merge = new Subcommand("merge") with RunnableCommand {
      val minAbundance = opt[Int](required = false, default = Some(1), descr = "Minimum k-mer abundance")
      val minLength = opt[Int](required = false, descr = "Minimum unitig length for output")
      val showStats = opt[Boolean](required = false, descr = "Show statistics for each merge iteration")

      def run() {
        val loc = location()
        val graph = routines.loadBucketGraph(loc, minAbundance.toOption.map(clipAbundance), None)
        val it = new IterativeMerge(spark, showStats(), minLength.toOption, k(), loc)
        it.iterate(graph)
      }
    }
    addSubcommand(merge)

    val stats = new HCCommand("stats") (
      routines.bucketStats(location())
    )
    addSubcommand(stats)
  }
  addSubcommand(buckets)

  verify()
  spark.sparkContext.setCheckpointDir(checkpoint())
}

object Hypercut extends SparkTool("Hypercut") {
  def main(args: Array[String]) {
    Commands.run(new HCSparkConf(args, spark))
  }
}
