package hypercut.shortread

import scala.io.BufferedSource
import friedrich.util.IO

object ReadFiles {
 def fastq(file: String): Iterator[String] =
    fastq(IO.source(file))

  def fastq(r: BufferedSource): Iterator[String] =
    r.getLines.grouped(4).map(_(1))

  def fasta(file: String): Iterator[String] =
    fasta(IO.source(file))

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
}