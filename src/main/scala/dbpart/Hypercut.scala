package dbpart

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.Subcommand
import dbpart.ubucket.BucketDB
import dbpart.ubucket.SeqBucketDB
import dbpart.graph.PathExtraction

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
  val numMarkers = opt[Int](required = true,
      descr = "Number of markers to extract from each k-mer", default = Some(4))
  val dbfile = opt[String](required = false,
      descr = "Path to database file (.kch) where sequences are stored")
  val minCov = opt[Int](descr = "Minimum coverage cutoff (does not affect the 'buckets build' command)")

  lazy val defaultBuckets = new SeqPrintBuckets(
    SeqPrintBuckets.space, k.toOption.get, numMarkers.toOption.get,
    dbfile.toOption.get, SeqBucketDB.options, minCov.toOption)

  val buckets = new Subcommand("buckets") {
    val build = new Subcommand("build") with RunnableCommand {
      banner("Hash reads and append them to a bucket database.")
      val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
      val mates = opt[String](descr = "Paired-end mates file (fastq, optionally .gz)")

      def run() {
        val spb = new SeqPrintBuckets(SeqPrintBuckets.space, k.toOption.get,
          numMarkers.toOption.get, dbfile.toOption.get, SeqBucketDB.mmapOptions, None)

        spb.build(input.toOption.get, mates.toOption)
        spb.stats
      }
    }
    addSubcommand(build)

    val list = new Subcommand("list") with RunnableCommand {
      banner("List the buckets in a database.")
      def run() { defaultBuckets.list }
    }
    addSubcommand(list)

    val show = new Subcommand("show") with RunnableCommand {
      banner("Show the sequences, coverages and edges contained in a bucket.")
      val buckets = trailArg[List[String]](required = true, descr = "Buckets to show")
      def run() {
        defaultBuckets.show(buckets.toOption.get)
      }
    }
    addSubcommand(show)

    val copy = new Subcommand("copy", "filter") with RunnableCommand {
      banner("Copy a bucket database into another one (appending), optionally applying coverage filtering. Edges are not coverage filtered.")
      val output = opt[String](required = true, descr = "Path to database file (.kch) to append into")
      def run() {
        //TODO: not copying edges yet
        val out = new SeqBucketDB(output.toOption.get, SeqBucketDB.options, k.toOption.get, None)
        out.copyAllFrom(defaultBuckets.db)
      }
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

  val analyse = new Subcommand("analyse") with RunnableCommand {
    banner("Analyse reads and display their fingerprints.")
    lazy val defaultExtractor = new MarkerSetExtractor(SeqPrintBuckets.space,
      numMarkers.toOption.get, k.toOption.get)

    val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz). Defaults to stdin.",
      default = Some("-"))

    def run() {
      defaultExtractor.prettyPrintMarkers(input.toOption.get)
    }
  }
  addSubcommand(analyse)

  val assemble = new Subcommand("assemble") with RunnableCommand {
    banner("Assemble contigs from a bucket database.")
    val output = opt[String](required = true,
        descr = "Output file (.fasta) to write generated sequences to",
        default = Some("hypercut.fasta"))
    val partitionSize = opt[Int](default = Some(100000), descr = "Desired partition size of macro graph")
    val minLength = opt[Int](default = Some(65), descr = "Minimum length of contigs to print")
    val partitionGraphs = toggle("partitionGraphs", default = Some(false),
      descrYes = "Output partition graphs as .dot files")
    val reasons = toggle("reasons", default = Some(false),
      descrYes = "Output reasons for contig endings")

    def run () {
      val extr = new PathExtraction(defaultBuckets, partitionSize.toOption.get,
        minLength.toOption.get, partitionGraphs.toOption.get,
        reasons.toOption.get)
      extr.printPathsByPartition()
    }
  }
  addSubcommand(assemble)

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