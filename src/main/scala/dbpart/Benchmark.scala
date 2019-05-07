package dbpart

import dbpart.shortread.ReadFiles
import dbpart.hash.MarkerSetExtractor
import dbpart.shortread.Read
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class BenchConf(args: Array[String]) extends CoreConf(args) {
  val input = opt[String](required = true, descr = "Input data file (fastq, optionally .gz)")
  val items = trailArg[List[String]]()
  val n = opt[Int](required = false, descr = "Number of items to test", default = Some(1000000))
  val threads = opt[Int](required = false, descr = "Number of threads", default = Some(1))
}

object Benchmark {
  def hashCode(x: String, k: Int) = {
    for (k <- Read.kmers(x, k)) { k.hashCode() }
  }

  def dry(x: String, k: Int) = {
    for (k <- Read.kmers(x, k)) {}
  }

  def extract(x: String, ext: MarkerSetExtractor) = {
    ext.markerSetsInRead(x)
  }

  def measure[A](conf: BenchConf, f: String => A) {
    val n = conf.n.toOption.get
    val thr = conf.threads.toOption.get

    val fs = (1 to thr).map(i => Future {
      val it = ReadFiles.iterator(conf.input.toOption.get).take(n)
      val start = System.currentTimeMillis()
      for (i <- it) {
        f(i)
      }
      val end = System.currentTimeMillis()
      val elapsed = end - start
      println(s"Finished in ${elapsed} ms")
      println("%.2f items/ms".format(n / elapsed.toDouble))
    })

    for (f <- fs) { Await.ready(f, Duration.Inf) }
  }

  def main(args: Array[String]) {
    val conf = new BenchConf(args)
    conf.verify

    println("Warmup")
    measure(conf, dry(_, conf.k.toOption.get))
    println("Warmup done")

    for (i <- conf.items.toOption.get) {
      println(s"Measuring $i")
      i match {
        case "hashCode" => measure(conf, hashCode(_, conf.k.toOption.get))
        case "dry"      => measure(conf, dry(_, conf.k.toOption.get))
        case "extract" =>
          val space = conf.defaultSpace
          val ext = MarkerSetExtractor.apply(space, conf.k.toOption.get)
          measure(conf, extract(_, ext))
      }
    }
  }
}