package hypercut.spark

import fastdoop.{FASTAshortInputFileFormat, FASTQInputFileFormat, FASTQReadsRecordReader, PartialSequence}
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
  def getShortReads(sc: SparkContext, file: String): RDD[String] = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[fastdoop.QRecord],
        sc.hadoopConfiguration)
      ss.map(_._2.getValue)
    } else {
      println(s"Assuming fasta format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[fastdoop.Record],
        sc.hadoopConfiguration)
      ss.map(_._2.getValue)
    }
  }
}
