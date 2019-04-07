package dbpart.bucketdb
import scala.collection.JavaConverters._
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }

/**
 * For use together with the CountingSeqBucket.
 * Coverages are stored in a separate database to improve insertion performance.
 * The coverage char at each position corresponds to the k-mer in the associated
 * CountingSeqBucket at the same position.
 */
final class CoverageBucket(val coverages: Iterable[String]) {
  import CountingSeqBucket._

  def kmerCoverages: Iterator[Int] = coverages.iterator.flatMap(_.map(covToInt))

  def sequenceCoverages: Iterable[Iterable[Int]] = coverages.map(_.map(covToInt))

  def average(xs: Iterable[Double]): Double = xs.sum/xs.size

  def sequenceAvgCoverages: Iterable[Double] =
    sequenceCoverages.map(sc => average(sc.map(_.toDouble)))

  def hasMinCoverage(min: Int) = kmerCoverages.exists(_ >= min)

  def pack: String = coverages.mkString(separator)
}

final class CoverageDB(val dbLocation: String, bnum: Int) extends StringKyotoDB[CoverageBucket] {
  import CountingSeqBucket._
  import SeqBucketDB._

  def unpack(key: String, value: String): CoverageBucket = {
    new CoverageBucket(value.split(separator, -1))
  }

  /**
   * 32 byte alignment
   * 8 GB mmap
   */
  def dbOptions: String = s"#bnum=$bnum#apow=5#mmap=$c8g#opts=l"

  var bulkData: CMap[String, CoverageBucket] = Map()
  def bulkLoad(keys: Iterable[String]) {
    bulkData = getBulk(keys)
  }

  def get(key: String): CoverageBucket = {
    bulkData.getOrElse(key, unpack(key, db.get(key)))
  }

  def set(key: String, bucket: CoverageBucket) {
    db.set(key, bucket.pack)
  }

  def getBulk(keys: Iterable[String]): CMap[String,CoverageBucket] = {
    var r = MMap[String,CoverageBucket]()
    visitBucketsReadonly(keys, (key, bucket) => { r += key -> bucket })
    r
  }

  def setBulk(data: CMap[String, CoverageBucket]) {
    db.set_bulk(data.map(x => (x._1 -> x._2.pack)).asJava, false)
  }
}