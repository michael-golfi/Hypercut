package dbpart.bucketdb
import scala.collection.JavaConverters._
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
import dbpart.bucket.CoverageBucket
import dbpart._

object StringCovBucket {
  val zeroCoverage: Char = '0'

  def asChar(cov: Int): Char = {
    (cov + zeroCoverage).toChar
  }

  def charToInt(cov: Char): Int = {
    (cov - zeroCoverage)
  }

  def fromCoverages(coverages: Seq[Array[Coverage]]): StringCovBucket =
    new StringCovBucket(coverages.map(x => new String(x.map(asChar(_)).toArray)).toArray)
}

/**
 * For use together with the CountingSeqBucket.
 * Coverages are stored in a separate database to improve insertion performance.
 * The coverage char at each position corresponds to the k-mer in the associated
 * CountingSeqBucket at the same position.
 *
 * This may eventually be retired and replaced with a byte-serialized verison of the
 * ShortCoverageBucket.
 */
final case class StringCovBucket(val coverageData: Array[String]) extends CoverageBucket {
  import PackedSeqBucket._
  import StringCovBucket._

  def coverages: Array[Array[Coverage]] = coverageData.map(_.toArray.map(x => charToInt(x).toShort))

  def pack: String = coverages.mkString(separator)
}

final class CoverageDB(val dbLocation: String, bnum: Int) extends StringKyotoDB[StringCovBucket] {
  import PackedSeqBucket._
  import SeqBucketDB._

  def unpack(key: String, value: String): StringCovBucket = {
    new StringCovBucket(value.split(separator, -1))
  }

  /**
   * 32 byte alignment
   * 8 GB mmap
   */
  def dbOptions: String = s"#bnum=$bnum#apow=5#mmap=$c8g#opts=l"

  var bulkData: CMap[String, StringCovBucket] = Map()
  def bulkLoad(keys: Iterable[String]) {
    bulkData = getBulk(keys)
  }

  def get(key: String): StringCovBucket = {
    bulkData.getOrElse(key, unpack(key, db.get(key)))
  }

  def set(key: String, bucket: StringCovBucket) {
    db.set(key, bucket.pack)
  }

  def getBulk(keys: Iterable[String]): CMap[String, StringCovBucket] = {
    var r = MMap[String,StringCovBucket]()
    visitBucketsReadonly(keys, (key, bucket) => { r += key -> bucket })
    r
  }

  def setBulk(data: CMap[String, StringCovBucket]) {
    db.set_bulk(data.map(x => (x._1 -> x._2.pack)).asJava, false)
  }
}