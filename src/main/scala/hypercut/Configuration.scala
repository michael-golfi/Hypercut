package hypercut

import org.rogach.scallop.Subcommand
import org.rogach.scallop.ScallopConf
import hypercut.hash.MotifSpace

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
  val numMotifs = opt[Int](
    required = true,
    descr = "Number of motifs to extract from each k-mer", default = Some(4))
  val space = opt[String](required = false, descr = "Motif space to use", default = Some("mixedTest"))

  val sample = opt[Double](required = false, descr = "Fraction of reads to sample for motif frequency",
      default = None)

  def defaultSpace = MotifSpace.named(space(), numMotifs())
}
