package hypercut.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.Subcommand
import hypercut._
import hypercut.hash.{MotifSetExtractor, MotifSpace, ReadSplitter}
import hypercut.taxonomic.TaxonomicIndex

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    //SparkConf can be customized here if needed
    new SparkConf
  }

  lazy val spark =
    SparkSession.builder().appName(appName).
      enableHiveSupport().
      master("spark://localhost:7077").config(conf).getOrCreate()

  lazy val routines = new Routines(spark)
}

class HCSparkConf(args: Array[String], spark: SparkSession) extends CoreConf(args) {
  version("Hypercut 0.1 beta (c) 2019-2020 Johan Nyström-Persson (Spark version)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  def routines = new Routines(spark)

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
    }
  }

  val kmers = new Subcommand("kmers") {
    val count = new RunnableCommand("count") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val output = opt[String](descr = "Location where outputs are written")
      val stats = opt[Boolean](default = Some(false),
        descr = "Output k-mer bucket stats (cannot be used with outputs)")
      val rawStats = opt[Boolean](default = Some(false),
        descr = "Output raw stats without counting k-mers (for debugging)", hidden = true)
      val segmentStats = opt[Boolean](default = Some(false),
        descr = "Output segment statistics (for benchmarking)", hidden = true)
      val precount = toggle(default = Some(false),
        descrYes = "Pre-group superkmers during shuffle before creating buckets")
      val sequence = toggle( default = Some(true),
        descrYes = "Output sequence for each k-mer in the counts table")
      val histogram = opt[Boolean](default = Some(false),
        descr = "Output a histogram instead of a counts table")
      val buckets = opt[Boolean](default = Some(false), descr = "Write buckets")

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
            } else if (segmentStats()) {
              counting.segmentStatsOnly(input)
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
        val index = new TaxonomicIndex(spark, spl, nodes())
        index.writeBuckets(input, labels(), location())
      }
    }
    addSubcommand(build)

    val classify = new RunnableCommand("classify") {
      val inFiles = trailArg[List[String]](required = true, descr = "Input sequence files")
      val unclassified = toggle( descrYes = "Output unclassified reads", default = Some(true))
      val detailed = toggle(descrYes = "Detailed taxon output in position order", default = Some(true))
      val output = opt[String](descr = "Output location", required = true)
      def run(): Unit = {
        val inData = inFiles().mkString(",")
        val hrf = new HadoopReadFiles(spark, k())
        val input = hrf.getReadsFromFilesWithID(inData, addRC(), true)
        val spl = restoreSplitter(location())
        val index = new TaxonomicIndex(spark, spl, nodes())
        index.classify(location(), input, k(), output(), unclassified(), !detailed())
      }
    }
    addSubcommand(classify)

    val stats = new RunnableCommand("stats") {
      def run(): Unit = {
        val spl = restoreSplitter(location())
        val index = new TaxonomicIndex(spark, spl, nodes())
        index.showIndexStats(location())
      }
    }
    addSubcommand(stats)

  }
  addSubcommand(taxonIndex)

  verify()
  spark.sparkContext.setCheckpointDir(checkpoint())
}

object Hypercut extends SparkTool("Hypercut") {
  def main(args: Array[String]) {
    Commands.run(new HCSparkConf(args, spark))
  }
}
