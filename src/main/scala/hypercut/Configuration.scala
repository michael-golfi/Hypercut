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

  val addRC = opt[Boolean](required = false, descr = "Add reverse complements")

  val hash = opt[String](required = false, descr = "Hash function to use (motifSet/prefix/minimizer)",
    default = Some("motifSet"))

  val numMotifs = opt[Int](
    required = true,
    descr = "MotifSet hash: Number of motifs to extract from each k-mer", default = Some(4))
  val space = opt[String](required = false, descr = "MotifSet hash: Motif space to use", default = Some("mixedTest"))

  val sample = opt[Double](required = false, descr = "MotifSet hash: Fraction of reads to sample for motif frequency",
      default = None)

  def preferredSpace = MotifSpace.named(space(), numMotifs())
}
