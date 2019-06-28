package dbpart.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

import dbpart.Commands
import dbpart.CoreConf
import dbpart.HCCommand
import dbpart.RunnableCommand
import dbpart.hash.MarkerSetExtractor

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
  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson (Spark version)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  def routines = new Routines(spark)

  val checkpoint = opt[String](required = false, default = Some("/ext/scratch/spark/checkpoint"),
      descr = "Path to checkpoint directory")

  val buckets = new Subcommand("buckets") {
    val location = opt[String](required = true, descr = "Path to location where buckets and edges are stored (parquet)")

    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true, descr = "Path to input data files")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
         val ext = new MarkerSetExtractor(defaultSpace, k.toOption.get)
         val minAbundance = None
         if (edges.toOption.get) {
           routines.graphFromReads(input.toOption.get, ext, location.toOption)
         } else {
           routines.bucketsOnly(input.toOption.get, ext, location.toOption.get)
         }
      }
    }
    addSubcommand(build)

    val partition = new Subcommand("partition") with RunnableCommand {
      val modulo = opt[Long](required = false, default = Some(10000), descr = "Modulo for BFS starting points")

      def run() {
        val eg = routines.loadEdgeGraph(location.toOption.get)
        routines.partitionBuckets(eg)(modulo.toOption.get)
      }
    }
    addSubcommand(partition)

    val stats = new HCCommand("stats") (
      routines.bucketStats(location.toOption.get)
    )
    addSubcommand(stats)
  }
  addSubcommand(buckets)

  verify()
  spark.sparkContext.setCheckpointDir(checkpoint.toOption.get)
}

object Hypercut extends SparkTool("Hypercut") {
  def main(args: Array[String]) {
    Commands.run(new HCSparkConf(args, spark))
  }
}
