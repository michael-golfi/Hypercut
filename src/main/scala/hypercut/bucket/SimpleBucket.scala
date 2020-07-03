package hypercut.bucket


import hypercut.hash.BucketId
import hypercut.spark.Counting
import miniasm.genome.bpbuffer.BPBuffer

import scala.collection.mutable
import scala.util.Sorting

object SimpleBucket {
  def fromCountedSequences(id: BucketId,
                           data: Array[(Array[Int], Long)]): SimpleBucket = {
    val kmers = data.map(_._1)
    val abunds = data.map(x => {
      x._2.min(Integer.MAX_VALUE).toInt
    })
    SimpleBucket(id, kmers, abunds)
  }
}

/**
 * Maintains a set of k-mers with abundances in sorted order.
 * @param id
 * @param kmers
 * @param abundances
 */
final case class SimpleBucket(id: BucketId,
                        kmers: Array[Array[Int]],
                        abundances: Array[Int]) {

}
