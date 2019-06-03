package dbpart

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.Subcommand

import dbpart.graph.PathExtraction
import dbpart.bucketdb.SeqBucketDB
import dbpart.hash.MarkerSpace
import dbpart.hash.MarkerSetExtractor

object Commands {
  def run(conf: ScallopConf) {
    for (com <- conf.subcommands) {
      com match {
      case command: RunnableCommand =>
        command.run
      case _ =>
      }
    }
  }
}

trait RunnableCommand {
  this: Subcommand =>

  def run(): Unit
}

class HCCommand(name: String)(act: => Unit) extends Subcommand(name) with RunnableCommand {
  def run() {
    act
  }
}

class CoreConf(args: Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](required = true, descr = "Length of each k-mer")
  val numMarkers = opt[Int](
    required = true,
    descr = "Number of markers to extract from each k-mer", default = Some(4))
  val space = opt[String](required = false, descr = "Marker space to use", default = Some("mixedTest"))

  def defaultSpace = MarkerSpace.named(space.toOption.get, numMarkers.toOption.get)
}

class Conf(args: Seq[String]) extends CoreConf(args) {
  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson (standalone)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  val dbfile = opt[String](required = false,
      descr = "Path to database file (.kch) where sequences are stored")
  val bnum = opt[Int](descr = "Number of buckets in kch databases (when creating new file)", default = Some(40000000))
  val minCov = opt[Coverage](descr = "Minimum coverage cutoff when reading databases")

  def defaultSettings = Settings.settings(dbfile.toOption.get, bnum.toOption.get)

  lazy val defaultBuckets = new SeqPrintBuckets(
    defaultSpace, k.toOption.get, numMarkers.toOption.get,
    defaultSettings, SeqBucketDB.options(bnum.toOption.get), minCov.toOption)

  val buckets = new Subcommand("buckets") {
    val build = new Subcommand("build") with RunnableCommand {
      banner("Hash reads and append them to a bucket database.")
      val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
      val mates = opt[String](descr = "Paired-end mates file (fastq, optionally .gz)")
      val index = toggle("index", default = Some(true), descrNo = "Do not index sequences")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
        val settings = if (index.toOption.get) {
          Settings.settings(dbfile.toOption.get, bnum.toOption.get)
        } else {
          Settings.noindexSettings(dbfile.toOption.get, bnum.toOption.get)
        }

        val spb = new SeqPrintBuckets(defaultSpace, k.toOption.get,
          numMarkers.toOption.get,
          settings, SeqBucketDB.mmapOptions(bnum.toOption.get),
          None)

        spb.build(input.toOption.get, mates.toOption, index.toOption.get, edges.toOption.get)
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
        val out = new SeqBucketDB(output.toOption.get, SeqBucketDB.options(bnum.toOption.get),
          bnum.toOption.get,
          k.toOption.get, None)
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
        defaultBuckets.checkConsistency(kmerCheck.toOption.get, seqCheck.toOption.get)
      }
    }
    addSubcommand(check)
  }
  addSubcommand(buckets)

  val analyse = new Subcommand("analyse") with RunnableCommand {
    banner("Analyse reads and display their fingerprints.")

    lazy val defaultExtractor = new MarkerSetExtractor(defaultSpace, k.toOption.get)

    val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz). Defaults to stdin.",
      default = Some("-"))
    val markers = toggle("markers", default = Some(false), descrYes = "Scan for raw markers in the reads and print a histogram")

    def run() {
      if (markers.toOption.get) {
       new FeatureScanner(defaultSpace).scan(input.toOption.get, None)
      } else {
        defaultExtractor.prettyPrintMarkers(input.toOption.get)
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
    Commands.run(new Conf(args))
  }
}