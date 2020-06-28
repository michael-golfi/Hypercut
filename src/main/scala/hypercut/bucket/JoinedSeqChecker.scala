package hypercut.bucket

import hypercut.hash.MotifSet
import hypercut.hash.MotifSpace

/**
 * Routines for checking the integrity of JoinedSeqBuckets.
 * @param space
 * @param k
 * @param kmerCheck
 * @param seqCheck
 */
class JoinedSeqChecker(space: MotifSpace, k: Int, kmerCheck: Boolean, seqCheck: Boolean) {
  import scala.collection.mutable.HashMap
  var errors = 0
  var count = 0

  /*
   * Check that each k-mer appears in only one bucket.
   * Expensive, memory intensive operation. Intended for debug purposes.
   */
  var map = new HashMap[String, String]()

  def checkBucket(key: String, bucket: JoinedSeqBucket[_]) {
    if (bucket.sequences.size > 100) {
      println(s"Warning: bucket $key contains ${bucket.sequences.size} sequences")
    }

    val numAbundances = bucket.abundances.size
    val numSeqs = bucket.sequences.size

    if (numAbundances != numSeqs) {
      Console.err.println(s"Error: bucket $key has $numSeqs sequences but $numAbundances abundances")
      errors += 1
    }

    /*
       * Also check the validity of each key
       */
    if (!checkKey(key)) {
      Console.err.println(s"Error: key $key is incorrect")
      errors += 1
    }

    if (kmerCheck) {
      for (kmer <- bucket.kmers) {
        count += 1
        if (map.contains(kmer)) {
          Console.err.println(s"Error: $kmer is contained in two buckets: $key, ${map(kmer)}")
          errors += 1
        }

        map += (kmer -> key)
        if (count % 10000 == 0) {
          print(".")
        }
      }
      val numKmers = bucket.kmers.size
      val numAbundances = bucket.abundances.map(_.length).sum
      if (numKmers != numAbundances) {
        Console.err.println(s"Error: bucket $key has $numKmers kmers but $numAbundances abundance positions")
        errors += 1
      }
    }

    if (seqCheck) {
      val endings = bucket.sequences.groupBy(s => s.substring(s.length() - (k - 1), s.length))
      for (s <- bucket.sequences) {
        val overlap = s.substring(0, k - 1)
        endings.get(overlap) match {
          case Some(ss) =>
            Console.err.println(s"Error: in bucket $key, sequence $s could be appended to: $ss")
            errors += 1
          case None =>
        }
      }
    }
  }

  def checkKey(key: String): Boolean = {
    try {
      val ms = MotifSet.unpack(space, key)
      if (ms.relativeMotifs(0).pos != 0) {
        return false
      }
      for (
        sub <- ms.relativeMotifs.sliding(2);
        if (sub.length >= 2)
      ) {
        val pos1 = sub(1).pos
        val l1 = sub(0).tag.length()
        if (pos1 < l1) {
          //Motifs cannot overlap
          return false
        }
      }
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }
}
