/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson 2010-2012.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */

package miniasm.genome.util

import scala.util.Random
import scala.annotation.tailrec

object DNAHelpers {

  /**
   * Obtain the complement of a single nucleotide.
   */
  def charComplement(bp: Char): Char = bp match {
    case 'A' => 'T'
    case 'C' => 'G'
    case 'T' => 'A'
    case 'G' => 'C'
    case 'N' => 'N'
    case _   => throw new Exception("Error: " + bp + " is not a nucleotide")
  }

  /**
   * Obtain the complement of a string of nucleotides.
   */
  def complement(data: String): String = {
    var i = 0
    val cs = new Array[Char](data.size)
    while (i < data.size) {
      cs(i) = charComplement(data.charAt(i))
      i += 1
    }
    new String(cs)
  }

  /**
   * Obtain the reverse complement of a string of nucleotides.
   */
  def reverseComplement(data: String): String = {
    complement(data.reverse)
  }

  /**
   * Extend a given string by a number of random basepairs
   */
  @tailrec
  final def extendSeq(seq: String,
                steps: Int,
                generator: Random = new Random(),
                basemap: Int => Char = Map(0 -> 'A',
                  1 -> 'C',
                  2 -> 'G',
                  3 -> 'T')): String = {
    if (steps == 0) {
      seq
    } else {
      extendSeq(seq + basemap(generator.nextInt(4)), steps - 1, generator, basemap)
    }
  }

  /**
   * Return a random sequence of basepairs as a string
   */
  def randomSequence(length: Int): String = extendSeq("", length)

  def kmerPrefix(seq: String, k: Int) = seq.substring(0, k - 1)
  def kmerPrefix(seq: StringBuilder, k: Int) = seq.substring(0, k - 1)

  def kmerSuffix(seq: String, k: Int) = seq.substring(seq.length() - (k - 1))
  def kmerSuffix(seq: StringBuilder, k: Int) = seq.substring(seq.size - (k - 1))

  def withoutPrefix(seq: String, k: Int) = seq.substring(k - 1)

  def withoutSuffix(seq: String, k: Int) = seq.substring(0, seq.length() - (k - 1))

}
