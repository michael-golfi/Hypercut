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
  val numMarkers = opt[Int](required = true, descr = "Number of markers to extract from each k-mer", default = Some(4))
  val dbfile = opt[String](required = true, descr = "Path to database file (.kch) where sequences are stored")
  val minCov = opt[Int](descr = "Minimum coverage cutoff (does not affect the 'buckets build' command)")

  lazy val defaultBuckets = new SeqPrintBuckets(
    SeqPrintBuckets.space, k.toOption.get, numMarkers.toOption.get,
    dbfile.toOption.get, BucketDB.options, minCov.toOption)

  val buckets = new Subcommand("buckets") {
    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
      val mates = opt[String](descr = "Paired-end mates file (fastq, optionally .gz)")

      def run() {
        val spb = new SeqPrintBuckets(SeqPrintBuckets.space, k.toOption.get,
          numMarkers.toOption.get, dbfile.toOption.get, BucketDB.mmapOptions, None)

        spb.build(input.toOption.get, mates.toOption)
        spb.stats
      }
    }
    addSubcommand(build)

    val show = new Subcommand("show") with RunnableCommand {
      val bucket = opt[String](required = true, descr = "Bucket to show")
      def run() {}
    }

    val list = new HCCommand("list")({
      ???
    })

    val copy = new Subcommand("copy") {
      val minCoverage = opt[Int](descr = "Coverage cutoff to filter buckets by when copying")
      val output = opt[String](required = true, descr = "Path to database file (.kch) to copy into")
    }
    addSubcommand(copy)

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

  val graph = new Subcommand("graph") with RunnableCommand {
    val minCoverage = opt[Int](descr = "Coverage cutoff for graph construction")
    val output = opt[String](required = true, descr = "Output file (.fasta) to write generated sequences to",
        default = Some("hypercut.fasta"))
    def run () {
      defaultBuckets.makeGraphFindPaths()
    }
  }
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