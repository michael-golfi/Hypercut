package hypercut.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
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
  val bgraph = new BucketGraph(routines)

  val checkpoint = opt[String](required = false, default = Some("/tmp/spark/checkpoint"),
      descr = "Path to checkpoint directory")

  def getSpace(inFiles: String, persistHashLocation: Option[String] = None): MotifSpace = {
    val input = getInputSequences(inFiles, long(), sample.toOption)
    sample.toOption match {
      case Some(amount) => routines.createSampledSpace(input, amount, preferredSpace, persistHashLocation)
      case None => preferredSpace
    }
  }

  def getInputSequences(input: String, longSequences: Boolean, sample: Option[Double] = None): Dataset[String] = {
    routines.getReadsFromFiles(input, addRC(), k(), sample, longSequences)
  }

  def restoreSplitter(location: String): ReadSplitter[_] = {
    hash() match {
      case "motifSet" =>
        val space = routines.restoreSpace(location, preferredSpace)
        new MotifSetExtractor(space, k(), distances())
      case _ => ???
    }
  }

  def getSplitter(inFiles: String, persistHash: Option[String] = None): ReadSplitter[_] = {
    hash() match {
      case "motifSet" =>
        new MotifSetExtractor(getSpace(inFiles, persistHash), k(), distances())
      case "minimizer" =>
        //The final parameter (B) currently has no effect
        new MinimizerSplitter(k(), numMotifs(), 2000)
    }
  }

  val kmers = new Subcommand("kmers") {
    val count = new RunnableCommand("count") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val output = opt[String](descr = "Location where outputs are written")
      val stats = opt[Boolean]("stats", default = Some(false), descr = "Output k-mer bucket stats (cannot be used with outputs)")
      val rawStats = opt[Boolean]("rawstats", default = Some(false), descr = "Output raw stats without counting k-mers (for debugging)")
      val precount = toggle("precount", default = Some(false), descrYes = "Pre-group superkmers during shuffle before creating buckets")
      val sequence = toggle("sequence", default = Some(true), descrYes = "Output sequence for each k-mer in the counts table")
      val histogram = opt[Boolean]("histogram", default = Some(false), descr = "Output a histogram instead of a counts table")
      val buckets = opt[Boolean]("buckets", default = Some(false), descr = "Write buckets")

      def run() {
        val inData = inFiles().mkString(",")
        val input = getInputSequences(inData, long())
        val spl = getSplitter(inData)
        val counting: Counting[_] = if (precount()) new GroupedCounting(spark, spl, addRC())
          else new SimpleCounting(spark, spl)

        output.toOption match {
          case Some(o) =>
            if (buckets()) {
              counting.writeBuckets(input, o)
            } else {
              counting.writeCountedKmers(input, sequence(), histogram(), o)
            }
          case _ =>
            if (stats() || rawStats()) {
              counting.statisticsOnly(input, rawStats())
            } else {
              ???
            }
        }
      }
    }
    addSubcommand(count)
  }
  addSubcommand(kmers)

  val taxonIndex = new Subcommand("taxonIndex") {
    val location = opt[String](required = true, descr = "Path to location where index is stored")
    val nodes = opt[String](descr = "Path to taxonomy nodes file", required = true)

    val build = new RunnableCommand("build") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val labels = opt[String](descr = "Path to sequence taxonomic label file", required = true)
      def run(): Unit = {
        val inData = inFiles().mkString(",")
        val hrf = new HadoopReadFiles(spark, k())
        val input = hrf.getReadsFromFilesWithID(inData, addRC())
        val spl = getSplitter(inData, Some(location()))
        val builder = new TaxonBucketBuilder(spark, spl, nodes())
        builder.writeBuckets(input, labels(), location())
      }
    }
    addSubcommand(build)

    val classify = new RunnableCommand("classify") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val output = opt[String](descr = "Output location", required = true)
      def run(): Unit = {
        val inData = inFiles().mkString(",")
        val hrf = new HadoopReadFiles(spark, k())
        val input = hrf.getReadsFromFilesWithID(inData, addRC())
        val spl = restoreSplitter(location())
        val builder = new TaxonBucketBuilder(spark, spl, nodes())
        builder.classify(location(), input, k(), output())

      }
    }
    addSubcommand(classify)

  }
  addSubcommand(taxonIndex)

  val buckets = new Subcommand("buckets") {
    val location = opt[String](required = true, descr = "Path to location where buckets and edges are stored (parquet)")

    val build = new RunnableCommand("build") {
      val input = trailArg[List[String]](required = true, descr = "Input sequence files")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
        val inFiles = input().mkString(",")
        val reads = getInputSequences(inFiles, false, None)
        val ext = new MotifSetExtractor(getSpace(inFiles), k())
        val minAbundance = None
        if (edges()) {
          bgraph.graphFromReads(reads, ext, addRC(), location.toOption)
        } else {
          bgraph.bucketsOnly(reads, ext, location.toOption, addRC())
        }
      }
    }
    addSubcommand(build)

    val top = new RunnableCommand("top") {
      val n = opt[Int](default = Some(10), descr = "Number of buckets to print")
      val amount = opt[Int](default = Some(10), descr = "Amount of sequence to print")
      //Note: could add a minAbundance flag here in the future if needed

      def run() {
       val buckets = bgraph.loadBuckets(location(), None)
       bgraph.showBuckets(buckets.map(_._2),
           n(), amount())
      }
    }
    addSubcommand(top)

    val merge = new RunnableCommand("merge") {
      val minAbundance = opt[Int](default = Some(1), descr = "Minimum k-mer abundance")
      val minLength = opt[Int](descr = "Minimum unitig length for output")
      val showStats = opt[Boolean](descr = "Show statistics for each merge iteration")
      val stopAtKmers = opt[Long](default = Some(1000000), descr = "Stop iterating at k-mer count")

      def run() {
        val loc = location()
        val graph = bgraph.loadBucketGraph(loc, minAbundance.toOption.map(clipAbundance), None)
        val it = new IterativeMerge(spark, showStats(), minLength.toOption, k(), loc)
        it.iterate(graph, stopAtKmers())
      }
    }
    addSubcommand(merge)

    val stats = new RunnableCommand("stats") {
      val minAbundance = opt[Int](default = Some(1), descr = "Minimum k-mer abundance")
      val stdout = opt[Boolean](default = Some(false), descr = "Print stats to stdout")
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
