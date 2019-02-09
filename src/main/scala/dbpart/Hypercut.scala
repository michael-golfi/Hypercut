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

  val k = opt[Int](required = true)
  val numMarkers = opt[Int](required = true)
  val minQuality = opt[Int]()

  val buckets = new Subcommand("buckets") {
    val dbfile = opt[String](required = true)

    lazy val defaultBuckets = new SeqPrintBuckets(
      SeqPrintBuckets.space, k.toOption.get, numMarkers.toOption.get,
      dbfile.toOption.get, BucketDB.options)

    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true)
      val mates = opt[String]()

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

  val graph = new Subcommand("graph") {

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