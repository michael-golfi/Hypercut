package hypercut

import hypercut.hash.{FeatureScanner, MotifSetExtractor}
import org.rogach.scallop.Subcommand

/**
 * Configuration for the standalone version of Hypercut, which runs without Spark.
 */
class Conf(args: Seq[String]) extends CoreConf(args) {
  version("Hypercut 0.1 beta (c) 2019-2020 Johan Nyström-Persson (standalone tool)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")


  val analyse = new RunnableCommand("analyse") {
    banner("Analyse reads and display their fingerprints.")

    lazy val defaultExtractor = new MotifSetExtractor(preferredSpace, k())

    val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz). Defaults to stdin.",
      default = Some("-"))
    val motifs = toggle("motifs", default = Some(false), descrYes = "Scan for raw motifs in the reads and print a histogram")

    def run() {
      if (motifs()) {
       new FeatureScanner(preferredSpace).scan(input())
      } else {
        defaultExtractor.prettyPrintMotifs(input())
      }
    }
  }
  addSubcommand(analyse)

  verify()
}

object Utility {
  def main(args: Array[String]) {
    Commands.run(new Conf(args))
  }
}