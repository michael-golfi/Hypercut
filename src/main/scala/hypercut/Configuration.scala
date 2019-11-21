package hypercut

import org.rogach.scallop.Subcommand
import org.rogach.scallop.ScallopConf
import hypercut.hash.MarkerSpace

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

/**
 * Configuration shared by the various tools implemented in this project.
 */
class CoreConf(args: Seq[String]) extends ScallopConf(args) {
  val k = opt[Int](required = true, descr = "Length of each k-mer")
  val numMarkers = opt[Int](
    required = true,
    descr = "Number of markers to extract from each k-mer", default = Some(4))
  val space = opt[String](required = false, descr = "Marker space to use", default = Some("mixedTest"))

  val sample = opt[Double](required = false, descr = "Fraction of reads to sample for marker frequency",
      default = None)

  def defaultSpace = MarkerSpace.named(space(), numMarkers())
}
