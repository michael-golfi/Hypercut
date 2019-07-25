package dbpart.bucketdb

import org.rogach.scallop.Subcommand

import dbpart._
import dbpart.RunnableCommand
import dbpart.graph.PathExtraction
import dbpart.hash.MarkerSetExtractor
import dbpart.hash.FeatureScanner

/**
 * Configuration for the standalone version of Hypercut, which runs without Spark.
 * Optionally writes data to a Kyoto Cabinet database.
 */
class Conf(args: Seq[String]) extends CoreConf(args) {
  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson (bucket DB tool)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  val dbfile = opt[String](required = false,
      descr = "Path to database file (.kch) where sequences are stored")
  val bnum = opt[Int](descr = "Number of buckets in kch databases (when creating new file)", default = Some(40000000))
  val minAbund = opt[Abundance](descr = "Minimum abundance cutoff when reading databases")

  def defaultSettings = Settings.settings(dbfile(), bnum())

  lazy val defaultBuckets = new BucketDBTool(
    defaultSpace, k(),
    defaultSettings, SeqBucketDB.options(bnum()), minAbund.toOption)

  val buckets = new Subcommand("buckets") {
    val build = new Subcommand("build") with RunnableCommand {
      banner("Hash reads and append them to a bucket database.")
      val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
      val mates = opt[String](descr = "Paired-end mates file (fastq, optionally .gz)")
      val index = toggle("index", default = Some(true), descrNo = "Do not index sequences")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
        val settings = if (index()) {
          Settings.settings(dbfile(), bnum())
        } else {
          Settings.noindexSettings(dbfile(), bnum())
        }

        val spb = new BucketDBTool(defaultSpace, k(),
          settings, SeqBucketDB.mmapOptions(bnum()),
          None)

        spb.build(input(), mates.toOption, index(), edges())
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
      banner("Show the sequences, abundances and edges contained in a bucket.")
      val buckets = trailArg[List[String]](required = true, descr = "Buckets to show")
      def run() {
        defaultBuckets.show(buckets())
      }
    }
    addSubcommand(show)

    val copy = new Subcommand("copy", "filter") with RunnableCommand {
      banner("Copy a bucket database into another one (appending), optionally applying abundance filtering. Edges are not abundance filtered.")
      val output = opt[String](required = true, descr = "Path to database file (.kch) to append into")
      def run() {
        val out = new SeqBucketDB(output(), SeqBucketDB.options(bnum()),
          bnum(),
          k(), None)
       //TODO implement or remove this command
       ???
      }
    }
    addSubcommand(copy)

    val stats = new HCCommand("stats")({
      defaultBuckets.stats
    })
    addSubcommand(stats)

    val check = new Subcommand("check") with RunnableCommand {
      val kmerCheck = toggle("kmers", default = Some(false),
        descrYes = "Check individual k-mers (memory intensive, slow)")

      val seqCheck = toggle("sequences", default = Some(false),
        descrYes = "Check sequence structure (slow)")

      def run() {
        defaultBuckets.checkConsistency(kmerCheck(), seqCheck())
      }
    }
    addSubcommand(check)
  }
  addSubcommand(buckets)

  val analyse = new Subcommand("analyse") with RunnableCommand {
    banner("Analyse reads and display their fingerprints.")

    lazy val defaultExtractor = new MarkerSetExtractor(defaultSpace, k())

    val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz). Defaults to stdin.",
      default = Some("-"))
    val markers = toggle("markers", default = Some(false), descrYes = "Scan for raw markers in the reads and print a histogram")

    def run() {
      if (markers()) {
       new FeatureScanner(defaultSpace).scan(input(), None)
      } else {
        defaultExtractor.prettyPrintMarkers(input())
      }
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
      val extr = new PathExtraction(defaultBuckets, partitionSize(),
        minLength(), partitionGraphs(),
        reasons())
      extr.printPathsByPartition()
    }
  }
  addSubcommand(assemble)

  verify()
}

object Hypercut {

  def main(args: Array[String]) {
    Commands.run(new Conf(args))
  }
}