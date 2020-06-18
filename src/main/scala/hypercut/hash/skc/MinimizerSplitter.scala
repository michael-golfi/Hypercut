package hypercut.hash.skc

import java.nio.{ByteBuffer, IntBuffer}

import hypercut.hash.ReadSplitter
import skc.util._

import scala.collection.mutable.ArrayBuffer

object MinimizerSplitter {
  var normCache: Array[Int] = null

  //NB m can only be set once for the lifetime of the JVM
  def getNorm(m: Int) = synchronized {
    if (normCache != null) normCache else {
      normCache = fillNorm(m)
      normCache
    }
  }
}


/**
  Splits reads based on minimizers.
  Based on code from "K-mer counting" by Sorella et al.
  Ferraro Petrillo, U., Sorella, M., Cattaneo, G. et al. Analyzing big datasets of genomic sequences:
    fast and scalable collection of k-mer statistics. BMC Bioinformatics 20, 2019.

  This code mostly comes from skc/SparkBinKmerCounter.scala in:
  https://bitbucket.org/maruscia/kmercounting/src/master/

 */
final case class MinimizerSplitter(k: Int, m: Int, B: Int) extends ReadSplitter[Int] {

//  private def bin(s: Int) = hash_to_bucket(s, B)
  //B is currently ignored
  private def bin(s: Int) = s

  //note: major source of memory usage for large m - should share between threads
  @transient
  private lazy val norm: Array[Int] = MinimizerSplitter.getNorm(m)

  private var min_s:Signature = Signature(-1,-1)
  private var super_kmer_start = 0
  private var s: Kmer = null
  private var N_pos = (-1, -1)
  private var i = 0

  private var lastMmask: Long = (1 << m * 2) - 1
  //keeps upper bound on distinct kmers that could be in a bin (for use with extractSuperKmersHT)

  /**
   * Split the read into superkmers overlapping by (k-1) bases.
   *
   * @param read
   * @return
   */
  override def split(read: String): Iterator[(Int, String)] = {
    var out = new ArrayBuffer[(Int, String)](read.length)
    val cur = read.getBytes

    if (cur.length >= k) {

      min_s = Signature(-1,-1)
      super_kmer_start = 0
      s = null
      N_pos = (-1, -1)
      i = 0

      while (i < read.length - k + 1) {
        N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N', cur, i, i + k) //DEBUG: not much influence


        if (N_pos._1 != -1) {
          //there's at least one 'N'

          if (super_kmer_start < i) {
            // must output a superkmer

            out += ((bin(min_s.value), read.substring(super_kmer_start, i - 1 + k)))
//            println("[out1] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))
          }
          super_kmer_start = i + N_pos._2 + 1 //after last index of N
          i += N_pos._2 + 1
        } else { //we have a valid kmer
          s = new Kmer(k, cur, i)

          if (i > min_s.pos) {
            if (super_kmer_start < i) {
              out += ((bin(min_s.value), read.substring(super_kmer_start, i - 1 + k)))
//              println("[out2] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))
              super_kmer_start = i
            }
            min_s.set(s.getSignature(m, norm), i)

          }
          else {
            val last = s.lastM(lastMmask, norm, m)

            if (last < min_s.value) {
              //add superkmer
              if (super_kmer_start < i) {
                out += ((bin(min_s.value), read.substring(super_kmer_start, i - 1 + k)))
//                println("[out3] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))
                super_kmer_start = i
              }

              min_s.set((last, i + k - m))
            }
          }

          i += 1
        }
      }

      if (cur.length - super_kmer_start >= k) {
        N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N', cur, i, cur.length)
        if (N_pos._1 == -1) {
          out += ((bin(min_s.value), read.substring(super_kmer_start, read.length)))
//          println("[out4] " + longToString(min_s.value) + " - " + new Kmer(cur.length - super_kmer_start, cur, super_kmer_start))

        }
        else if (i + N_pos._1 >= super_kmer_start + k) {
          out += ((bin(min_s.value), read.substring(super_kmer_start, i + N_pos._1 + super_kmer_start)))
//          println("[out5] " + longToString(min_s.value) + " - " + new Kmer(N_pos._1, cur, super_kmer_start))
        }
      }
    }

    out.iterator
  }

  /**
   * Convert a hashcode into a compact representation.
   *
   * @param hash
   * @return
   */
  override def compact(hash: Int): Long = hash.toLong
//    Array((hash & 255).toByte,
//      ((hash >> 8) & 255).toByte,
//      ((hash >> 16) & 255).toByte,
//      (hash >> 24).toByte
  // )

}
