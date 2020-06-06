package hypercut.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand
import hypercut._
import hypercut.hash.{MotifSetExtractor, MotifSpace, ReadSplitter}
import hypercut.bucket.AbundanceBucket._
import hypercut.hash.skc.MinimizerSplitter

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

  version("Hypercut 0.1 beta (c) 2019-2020 Johan Nyström-Persson (Spark version)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  def routines = new Routines(spark)
  val counting = new Counting(spark)
  val bgraph = new BucketGraph(routines)

  val checkpoint = opt[String](required = false, default = Some("/tmp/spark/checkpoint"),
      descr = "Path to checkpoint directory")

  def getSpace(input: String): MotifSpace = {
    sample.toOption match {
      case Some(amount) => routines.createSampledSpace(input, amount, preferredSpace)
      case None => preferredSpace
    }
  }

  def getSplitter(input: String): ReadSplitter[_] = {
    hash() match {
      case "motifSet" =>
        new MotifSetExtractor(getSpace(input), k())
      case "minimizer" =>
        //The final parameter (B) currently has no effect
        new MinimizerSplitter(k(), numMotifs(), 2000)
    }
  }

  val kmers = new Subcommand("kmers") {
    val count = new RunnableCommand("count") {
      val input = opt[String](required = true, descr = "Path to input data files")
      val output = opt[String](required = false, descr = "Path to location where the kmer count table is written")
      val stats = opt[Boolean]("stats", default = Some(false), descr = "Output k-mer bucket stats (cannot be used with outputs)")
      val rawStats = opt[Boolean]("rawstats", default = Some(false), descr = "Output raw stats without counting k-mers")
      val precount = toggle(name = "precount", default = Some(true), descrYes = "Pre-group superkmers during shuffle before creating buckets")
      val sequence = toggle(name = "sequence", default = Some(true), descrYes = "Output sequence for each k-mer in the histogram")

      def run() {
        val spl = getSplitter(input())
        output.toOption match {
          case Some(o) =>
            counting.writeCountedKmers(spl, input(), addRC(), precount(), sequence(), o)
          case _ =>
            if (stats() || rawStats()) {
              counting.statisticsOnly(spl, input(), addRC(), precount(), rawStats())
            } else {
              ???
            }
        }
      }
    }
    addSubcommand(count)
  }
  addSubcommand(kmers)

  val buckets = new Subcommand("buckets") {
    val location = opt[String](required = true, descr = "Path to location where buckets and edges are stored (parquet)")

    val build = new RunnableCommand("build") {
      val input = opt[String](required = true, descr = "Path to input data files")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
         val ext = new MotifSetExtractor(getSpace(input()), k())
         val minAbundance = None
         if (edges()) {
           bgraph.graphFromReads(input(), ext, location.toOption)
         } else {
           bgraph.bucketsOnly(input(), ext, location.toOption, addRC())
         }
      }
    }
    addSubcommand(build)

    val top = new RunnableCommand("top") {
      val n = opt[Int](required = false, default = Some(10),
          descr = "Number of buckets to print")
      val amount = opt[Int](required = false, default = Some(10),
          descr = "Amount of sequence to print")
      //Note: could add a minAbundance flag here in the future if needed

      def run() {
       val buckets = bgraph.loadBuckets(location(), None)
       bgraph.showBuckets(buckets.map(_._2),
           n(), amount())
      }
    }
    addSubcommand(top)

    val merge = new RunnableCommand("merge") {
      val minAbundance = opt[Int](required = false, default = Some(1), descr = "Minimum k-mer abundance")
      val minLength = opt[Int](required = false, descr = "Minimum unitig length for output")
      val showStats = opt[Boolean](required = false, descr = "Show statistics for each merge iteration")
      val stopAtKmers = opt[Long](required = false, default = Some(1000000), descr = "Stop iterating at k-mer count")

      def run() {
        val loc = location()
        val graph = bgraph.loadBucketGraph(loc, minAbundance.toOption.map(clipAbundance), None)
        val it = new IterativeMerge(spark, showStats(), minLength.toOption, k(), loc)
        it.iterate(graph, stopAtKmers())
      }
    }
    addSubcommand(merge)

    val stats = new RunnableCommand("stats") {
      val minAbundance = opt[Int](required = false, default = Some(1), descr = "Minimum k-mer abundance")
      val stdout = opt[Boolean](required = false, default = Some(false), descr = "Print stats to stdout")
      def run() {
        bgraph.bucketStats(location(), minAbundance.toOption.map(clipAbundance), stdout())
      }
    }

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
