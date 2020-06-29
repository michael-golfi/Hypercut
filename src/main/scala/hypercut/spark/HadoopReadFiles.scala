package hypercut.spark

import fastdoop._
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HadoopReadFiles {

  /**
   * Using the fastdoop library, read short read sequence data only from the input file.
   * @param sc
   * @param file
   * @return
   */
  def getShortReads(sc: SparkContext, file: String, k: Int): RDD[String] = {
    val conf = sc.hadoopConfiguration
    conf.set("k", k.toString)
    conf.set("look_ahead_buffer_size", (1024 * 1024).toString)

    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        sc.hadoopConfiguration)
      ss.map(_._2.getValue.replaceAll("\n", ""))
    } else {
      println(s"Assuming fasta format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        sc.hadoopConfiguration)
      ss.map(_._2.getValue.replaceAll("\n", ""))
    }
  }

  def getLongSequence(sc: SparkContext, file: String, k: Int): RDD[String] = {
    println(s"Assuming fasta format (long sequences) for $file")
    val ss = sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      sc.hadoopConfiguration)
    ss.map(_._2.getValue.replaceAll("\n", ""))
  }
}
