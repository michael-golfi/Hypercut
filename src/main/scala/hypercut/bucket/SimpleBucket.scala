package hypercut.bucket

import hypercut.Abundance
import hypercut.hash.BucketId
import org.apache.spark.sql.Dataset

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
 * Maintains a set of k-mers in sorted order.
 * @param id
 * @param kmers
 * @param abundances
 */
case class SimpleBucket(id: BucketId,
                        kmers: Array[Array[Int]],
                        abundances: Array[Int]) {

}
