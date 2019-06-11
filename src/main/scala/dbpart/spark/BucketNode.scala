package dbpart.spark

import dbpart.bucket.SimpleCountingBucket

/**
 * Represents a single bucket in the macro graph for the purpose of partitioning.
 */
final case class BucketNode(partition: Long = -1, isBoundary: Boolean = true) {

  def inPartition = (partition != -1)

}