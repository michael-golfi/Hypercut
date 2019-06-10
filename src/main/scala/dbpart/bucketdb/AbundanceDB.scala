package dbpart.bucketdb
import scala.collection.JavaConverters._
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
import dbpart.bucket.AbundanceBucket
import dbpart._

/**
 * Represents a series of abundance points as a string.
 */
object StringAbundanceBucket {
  val zeroAbundance: Char = '0'

  def asChar(abund: Int): Char = {
    (abund + zeroAbundance).toChar
  }

  def charToInt(abund: Char): Int = {
    (abund - zeroAbundance)
  }

  def fromAbundances(abundances: Seq[Array[Abundance]]): StringAbundanceBucket =
    new StringAbundanceBucket(abundances.map(x => new String(x.map(asChar(_)).toArray)).toArray)
}

/**
 * For use together with the CountingSeqBucket.
 * Abundances are stored in a separate database to improve insertion performance.
 * The abundance char at each position corresponds to the k-mer in the associated
 * CountingSeqBucket at the same position.
 *
 * This may eventually be retired and replaced with a byte-serialized verison of the
 * ShortAbundanceBucket.
 */
final case class StringAbundanceBucket(val abundanceData: Array[String]) extends AbundanceBucket {
  import PackedSeqBucket._
  import StringAbundanceBucket._

  def abundances: Array[Array[Abundance]] = abundanceData.map(_.toArray.map(x => charToInt(x).toShort))

  def pack: String = abundances.mkString(separator)
}

final class AbundanceDB(val dbLocation: String, bnum: Int) extends StringKyotoDB[StringAbundanceBucket] {
  import PackedSeqBucket._
  import SeqBucketDB._

  def unpack(key: String, value: String): StringAbundanceBucket = {
    new StringAbundanceBucket(value.split(separator, -1))
  }

  /**
   * 32 byte alignment
   * 8 GB mmap
   */
  def dbOptions: String = s"#bnum=$bnum#apow=5#mmap=$c8g#opts=l"

  var bulkData: CMap[String, StringAbundanceBucket] = Map()
  def bulkLoad(keys: Iterable[String]) {
    bulkData = getBulk(keys)
  }

  def get(key: String): StringAbundanceBucket = {
    bulkData.getOrElse(key, unpack(key, db.get(key)))
  }

  def set(key: String, bucket: StringAbundanceBucket) {
    db.set(key, bucket.pack)
  }

  def getBulk(keys: Iterable[String]): CMap[String, StringAbundanceBucket] = {
    var r = MMap[String,StringAbundanceBucket]()
    visitBucketsReadonly(keys, (key, bucket) => { r += key -> bucket })
    r
  }

  def setBulk(data: CMap[String, StringAbundanceBucket]) {
    db.set_bulk(data.map(x => (x._1 -> x._2.pack)).asJava, false)
  }
}