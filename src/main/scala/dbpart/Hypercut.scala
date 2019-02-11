package dbpart

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.Subcommand
import dbpart.ubucket.BucketDB

trait RunnableCommand {
  this: Subcommand =>

  def run(): Unit
}

class HCCommand(name: String)(act: => Unit) extends Subcommand(name) with RunnableCommand {
  def run() {
    act
  }
}

class Conf(args: Seq[String]) extends ScallopConf(args) {
  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  val k = opt[Int](required = true, descr = "Length of each k-mer")
  val numMarkers = opt[Int](required = true, descr = "Number of markers to extract from each k-mer")
  val dbfile = opt[String](required = true, descr = "Path to database file (.kch) where sequences are stored")

  lazy val defaultBuckets = new SeqPrintBuckets(
    SeqPrintBuckets.space, k.toOption.get, numMarkers.toOption.get,
    dbfile.toOption.get, BucketDB.options)

  val buckets = new Subcommand("buckets") {
    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
      val mates = opt[String](descr = "Paired-end mates file (fastq, optionally .gz)")

      def run() {
        val spb = new SeqPrintBuckets(SeqPrintBuckets.space, k.toOption.get,
          numMarkers.toOption.get, dbfile.toOption.get, BucketDB.mmapOptions)

        spb.build(input.toOption.get, mates.toOption)
        spb.stats
      }
    }
    addSubcommand(build)

    val stats = new HCCommand("stats")({
      defaultBuckets.stats
    })
    addSubcommand(stats)

    val check = new HCCommand("check")({
      defaultBuckets.checkConsistency()
    })
    addSubcommand(check)
  }
  addSubcommand(buckets)

  val graph = new HCCommand("graph")({
    val minCoverage = opt[Int](descr = "Coverage cutoff for graph construction")
    defaultBuckets.makeGraphFindPaths()
  })
  addSubcommand(graph)

  verify()
}

object Hypercut {
  def main(args: Array[String]) {
    val conf = new Conf(args)

    for (com <- conf.subcommands) {
      com match {
      case command: RunnableCommand =>
        command.run
      case _ =>
      }
    }
  }
}