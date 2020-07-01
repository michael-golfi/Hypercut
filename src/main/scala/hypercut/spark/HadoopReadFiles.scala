package hypercut.spark

import fastdoop._
import miniasm.genome.util.DNAHelpers
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

class HadoopReadFiles(spark: SparkSession, k: Int) {

  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  val conf = sc.hadoopConfiguration
  conf.set("k", k.toString)
  conf.set("look_ahead_buffer_size", (1024 * 1024).toString)

  /**
   * Using the fastdoop library, read short read sequence data only from the input file.
   * @param sc
   * @param file
   * @return
   */
  def getShortReads(file: String): RDD[String] = {
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

  // Returns (id, sequence)
  def getShortReadsWithID(file: String): RDD[(String, String)] = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        sc.hadoopConfiguration)
      ss.map(r => (r._2.getKey.split("\\s+")(0), r._2.getValue.replaceAll("\n", "")))
    } else {
      println(s"Assuming fasta format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        sc.hadoopConfiguration)
      ss.map(r => (r._2.getKey.split("\\s+")(0), r._2.getValue.replaceAll("\n", "")))
    }
  }

  def getLongSequence(file: String): RDD[String] = {
    println(s"Assuming fasta format (long sequences) for $file")
    val ss = sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      sc.hadoopConfiguration)
    ss.map(_._2.getValue.replaceAll("\n", ""))
  }

  /**
   * Load sequences from files, optionally adding reverse complements and/or sampling.
   */
  def getReadsFromFiles(fileSpec: String, withRC: Boolean,
                        sample: Option[Double] = None,
                        longSequence: Boolean = false): Dataset[String] = {
    val raw = if(longSequence)
      getLongSequence(fileSpec).toDS
    else
      getShortReads(fileSpec).toDS

    //See https://sg.idtdna.com/pages/support/faqs/what-are-the-base-degeneracy-codes-that-you-use-(eg.-r-w-k-v-s)-
    val degenerate = "[RYMKSWHBVDN]+"

    val sampled = sample match {
      case Some(s) => raw.sample(s)
      case _ => raw
    }

    val valid = sampled.flatMap(r => r.split(degenerate))

    if (withRC) {
      valid.flatMap(r => {
        Seq(r, DNAHelpers.reverseComplement(r))
      })
    } else {
      valid
    }
  }

  /**
   * Load reads with their IDs from DNA files.
   * @param fileSpec
   * @param withRC
   * @param sample
   * @param longSequence
   * @return
   */
  def getReadsFromFilesWithID(fileSpec: String,
                        longSequence: Boolean = false): Dataset[(String, String)] = {
    val raw = if(longSequence)
      ???
    else
      getShortReadsWithID(fileSpec).toDS

    //See https://sg.idtdna.com/pages/support/faqs/what-are-the-base-degeneracy-codes-that-you-use-(eg.-r-w-k-v-s)-
    val degenerate = "[RYMKSWHBVDN]+"

    raw.flatMap(r => r._2.split(degenerate).map(s => (r._1, s)))
  }

}
