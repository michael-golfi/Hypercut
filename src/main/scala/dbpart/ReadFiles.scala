package dbpart

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

  def iterator(file: String): Iterator[String] = {
    if (file.endsWith(".fq") || file.endsWith(".fastq")) {
     println("Assuming fastq format")
     fastq(file)
   } else if (file.endsWith(".fa") || file.endsWith(".fasta")) {
     println("Assuming fasta format")
     fasta(file)
   } else {
     throw new Exception(s"Unknown file format: $file")
   }
 }
}