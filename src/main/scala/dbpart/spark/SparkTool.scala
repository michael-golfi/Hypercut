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
      classOf[SimpleCountingBucket], classOf[CompactNode],
      classOf[Array[SimpleCountingBucket]], classOf[Array[CompactNode]],

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
