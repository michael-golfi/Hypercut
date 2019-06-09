package dbpart.spark

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.Subcommand

import dbpart.Commands
import dbpart.CoreConf
import dbpart.HCCommand
import dbpart.RunnableCommand
import dbpart.bucket.SimpleCountingBucket
import dbpart.hash.CompactNode
import dbpart.hash.MarkerSetExtractor

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    val conf = new SparkConf

    conf.registerKryoClasses(Array(
      classOf[SimpleCountingBucket], classOf[CompactNode], classOf[BucketNode],
      classOf[Array[SimpleCountingBucket]], classOf[Array[CompactNode]], classOf[Array[BucketNode]],

      classOf[String], classOf[Array[String]], classOf[Array[Short]], classOf[Array[Array[Short]]],
      classOf[Array[Byte]], classOf[Array[Array[Byte]]], classOf[Tuple2[Any, Any]],
      classOf[Array[Long]],

      //Hidden or inaccessible classes
      Class.forName("scala.reflect.ManifestFactory$$anon$8"),
      Class.forName("scala.reflect.ManifestFactory$$anon$9"),
      Class.forName("scala.reflect.ManifestFactory$$anon$10"),
      Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
      Class.forName("org.apache.spark.sql.types.LongType$"),
      Class.forName("org.apache.spark.sql.types.ShortType$"),
      Class.forName("org.apache.spark.sql.types.IntegerType$"),
      Class.forName("org.apache.spark.sql.types.StringType$"),
      Class.forName("org.apache.spark.graphx.impl.ShippableVertexPartition"),
      Class.forName("org.apache.spark.graphx.impl.RoutingTablePartition"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$mcJI$sp"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$1"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$2"),
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      Class.forName("org.apache.spark.util.collection.OpenHashSet$LongHasher"),
      Class.forName("scala.reflect.ClassTag$$anon$1"),

      classOf[org.apache.spark.sql.types.StructType],
      classOf[Array[org.apache.spark.sql.types.StructType]],
      classOf[org.apache.spark.sql.types.StructField],
      classOf[Array[org.apache.spark.sql.types.StructField]],
      classOf[org.apache.spark.sql.types.Metadata],
      classOf[org.apache.spark.sql.types.ArrayType],
      classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
      classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
      classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
      classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
      classOf[org.apache.spark.sql.catalyst.InternalRow],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]]))
    GraphXUtils.registerKryoClasses(conf)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf
  }

  lazy val spark = SparkSession.builder().appName(appName).
      master("spark://localhost:7077").config(conf).getOrCreate()

  lazy val routines = new Routines(spark)
}

class HCSparkConf(args: Array[String], spark: SparkSession) extends CoreConf(args) {
  version("Hypercut 0.1 beta (c) 2019 Johan Nyström-Persson (Spark version)")
  banner("Usage:")
  footer("Also see the documentation (to be written).")

  def routines = new Routines(spark)

  val buckets = new Subcommand("buckets") {
    val location = opt[String](required = true, descr = "Path to location where buckets and edges are stored (parquet)")

    val build = new Subcommand("build") with RunnableCommand {
      val input = opt[String](required = true, descr = "Path to input data files")
      val edges = toggle("edges", default = Some(true), descrNo = "Do not index edges")

      def run() {
         val ext = new MarkerSetExtractor(defaultSpace, k.toOption.get)
         val minCoverage = None
         if (edges.toOption.get) {
           routines.buildBuckets(input.toOption.get, ext, minCoverage, location.toOption)
         } else {
           routines.countKmers(input.toOption.get, ext, location.toOption.get)
         }
      }
    }
    addSubcommand(build)

    val partition = new Subcommand("partition") with RunnableCommand {
      val modulo = opt[Long](required = false, default = Some(10000), descr = "Modulo for BFS starting points")

      def run() {
        val eg = routines.loadEdgeGraph(location.toOption.get)
        routines.partitionBuckets(eg)(modulo.toOption.get)
      }
    }
    addSubcommand(partition)

    val stats = new HCCommand("stats") (
      routines.bucketStats(location.toOption.get)
    )
    addSubcommand(stats)
  }
  addSubcommand(buckets)

  verify()
}

object Hypercut extends SparkTool("Hypercut") {
  def main(args: Array[String]) {
    Commands.run(new HCSparkConf(args, spark))
  }
}
