package hypercut

import org.rogach.scallop.Subcommand
import org.rogach.scallop.ScallopConf
import hypercut.hash.{MotifSetExtractor, MotifSpace}

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

abstract class RunnableCommand(title: String) extends Subcommand(title) {
  def run(): Unit
}

class HCCommand(name: String)(act: => Unit) extends RunnableCommand(name) {
  def run() {
    act
  }
}

/**
 * Configuration shared by the various tools implemented in this project.
 */
class CoreConf(args: Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](required = true, descr = "Length of each k-mer")

  val addRC = opt[Boolean](name = "addRC", descr = "Add reverse complements")

  val hash = opt[String](descr = "Hash function to use (motifSet/minimizer/signature)",
    default = Some("motifSet"), hidden = true)

  val numMotifs = opt[Int](
    descr = "MotifSet hash: Number of motifs to extract from each k-mer", default = Some(1))

  val space = opt[String](descr = "MotifSet hash: Motif space to use", default = Some("mixedTest"))

  val width = opt[Int](descr = "Width of motifs", default = Some(10))

  val sample = opt[Double](descr = "Fraction of reads to sample for motif frequency",
    required = true, default = Some(0.01))

  val numCPUs = opt[Int](name = "numCPUs",
    descr = "Number of CPUs expected to be available to executors",
    required = false, default = Some(16))

  val motifList = opt[String](descr = "List of motifs to use")

  val distances = toggle(descrYes = "MotifSet hash: Include distances", default = Some(true))

  val rna = opt[Boolean](descr = "RNA mode (default is DNA)", default = Some(false))

  val long = toggle(default = Some(false), descrYes = "Read long sequence instead of short reads")

  def preferredSpace = {
    width.toOption match {
      case Some(w) => MotifSpace.ofLength(w, numMotifs(), rna(), "default")
      case None => MotifSpace.named(space(), numMotifs())
    }
  }
}
