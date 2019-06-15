package dbpart.graph
import scala.collection.{Map => CMap}
import dbpart.bucket.CountingSeqBucket

/**
 * A method for loading data contained in buckets identified by String keys.
 */
trait BucketLoader {
  def getBuckets(keys: Iterable[String]): CMap[String, CountingSeqBucket[_]]
}