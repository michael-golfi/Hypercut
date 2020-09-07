package hypercut.shortread

import java.io.{BufferedWriter, FileInputStream, FileOutputStream, InputStream, OutputStream, OutputStreamWriter}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.io.{BufferedSource, Source}

object ReadFiles {
 def fastq(file: String): Iterator[String] =
    fastq(source(file))

  def fastq(r: BufferedSource): Iterator[String] =
    r.getLines.grouped(4).map(_(1))

  def fasta(file: String): Iterator[String] =
    fasta(source(file))

  def fasta(r: BufferedSource): Iterator[String] =
    r.getLines.grouped(2).map(_(1))

  def iterator(rawFile: String): Iterator[String] = {
    val file = if (rawFile.endsWith(".gz")) rawFile.dropRight(3) else rawFile

    if (file.endsWith(".fq") || file.endsWith(".fastq") || file == "-") {
     println("Assuming fastq format")
     fastq(rawFile)
   } else if (file.endsWith(".fa") || file.endsWith(".fasta")) {
     println("Assuming fasta format")
     fasta(rawFile)
   } else {
     throw new Exception(s"Unknown file format: $rawFile")
   }
 }

  /**
   * Intelligently obtain a file reader, transparently performing
   * transformations such as compression.
   */
  def source(file: String): BufferedSource = {
    if (file == "-") {
      Source.stdin
    } else {
      var stream: InputStream = new FileInputStream(file)
      if (file.endsWith(".gz")) {
        stream = new GZIPInputStream(stream)
      }
      new BufferedSource(stream)
    }
  }

  /**
   * Intelligently obtain a file writer, transparently performing
   * transformations such as compression.
   */
  def writer(file: String): BufferedWriter = {
    var stream: OutputStream = new FileOutputStream(file)
    if (file.endsWith(".gz")) {
      stream = new GZIPOutputStream(stream)
    }
    new BufferedWriter(new OutputStreamWriter(stream))
  }
}