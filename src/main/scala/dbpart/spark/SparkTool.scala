package dbpart.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils
import dbpart.bucket.SimpleCountingBucket
import dbpart.hash.CompactNode

abstract class SparkTool(appName: String) {
  def conf: SparkConf = {
    val conf = new SparkConf

    conf.registerKryoClasses(Array(
      classOf[SimpleCountingBucket], classOf[CompactNode], classOf[PathNode],
      classOf[Array[SimpleCountingBucket]], classOf[Array[CompactNode]], classOf[Array[PathNode]],

      classOf[String], classOf[Array[String]], classOf[Array[Short]], classOf[Array[Array[Short]]],
      classOf[Array[Byte]], classOf[Array[Array[Byte]]], classOf[Tuple2[Any, Any]],
      classOf[Array[Long]],

      //Hidden or inaccessible classes
      Class.forName("scala.reflect.ManifestFactory$$anon$8"),
      Class.forName("scala.reflect.ManifestFactory$$anon$9"),
      Class.forName("scala.reflect.ManifestFactory$$anon$10"),
      Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
      Class.forName("org.apache.spark.graphx.impl.ShippableVertexPartition"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$mcJI$sp"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$1"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$2"),
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
      Class.forName("org.apache.spark.util.collection.OpenHashSet$LongHasher"),
      Class.forName("scala.reflect.ClassTag$$anon$1"),

      classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
      classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
      classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow]))
    GraphXUtils.registerKryoClasses(conf)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf
  }

  lazy val spark = SparkSession.builder().appName(appName).
      master("spark://localhost:7077").config(conf).getOrCreate()

  lazy val routines = new Routines(spark)
}
