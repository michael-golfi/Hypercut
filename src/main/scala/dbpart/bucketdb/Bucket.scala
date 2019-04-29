package dbpart.bucketdb

import dbpart._
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuilder
import java.util.Arrays
import dbpart.shortread.Read
import dbpart.bucket.CoverageBucket
import dbpart.bucket.CountingSeqBucket

trait Unpacker[Packed, B <: Bucket[Packed, B]] {
  def unpack(key: Packed, value: Packed, k: Int): B
}

trait Bucket[Packed, +B <: Bucket[Packed, B]] {
  def pack: Packed
  def size: Int
  def items: Iterable[Packed]

  /**
   * Insert a single into the bucket, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertSingle(value: Packed): Option[B]

  /**
   * Insert a number of values into the bucket, returning the updated set if an update was performed
   * Returns None if no update is needed.
   */
  def insertBulk(values: Iterable[Packed]): Option[B]
}

/**
 * Stores distinct strings (not necessarily NT sequences).
 * The value of k has no meaning for this implementation.
 */
object DistinctBucket extends Unpacker[String, DistinctBucket] {
  val separator: String = "\n"

  def unpack(key: String, set: String, k: Int): DistinctBucket =
    new DistinctBucket(set, set.split(separator, -1).toList)
}

final class DistinctBucket(oldSet: String, val items: List[String])
  extends Bucket[String, DistinctBucket] {
  import DistinctBucket._
  def size: Int = items.size

  def insertSingle(value: String): Option[DistinctBucket] = {
    if (!items.contains(value)) {
      Some(new DistinctBucket(s"$oldSet$separator$value", value :: items))
    } else {
      None
    }
  }

  def insertBulk(values: Iterable[String]): Option[DistinctBucket] = {
    val newVals = values.filter(!items.contains(_)).toList.distinct
    if (!newVals.isEmpty) {
      val newString = newVals.mkString(separator)
      Some(new DistinctBucket(s"$oldSet$separator$newString", newVals ::: items))
    } else {
      None
    }
  }

  def pack: String = oldSet
}

object DistinctByteBucket {
  type Rec = Array[Byte]

  class Unpacker(val itemSize: Int) extends dbpart.bucketdb.Unpacker[Array[Byte], DistinctByteBucket] {

    def unpack(key: Rec, set: Rec, k: Int): DistinctByteBucket = {
      var i = 0
      var items = List[Seq[Byte]]()
      while (i < set.length) {
        items ::= Arrays.copyOfRange(set, i, i + itemSize).toSeq
        i += itemSize
      }
      new DistinctByteBucket(set, items, itemSize)
    }
  }
}

/**
 * Distinct bucket for byte arrays.
 * Arrays do not have deep equality by default, so we also maintain them internally
 * as Seq[Byte] (which do).
 */
final class DistinctByteBucket(oldSet: Array[Byte], separated: List[Seq[Byte]],
  val itemSize: Int)
  extends Bucket[Array[Byte], DistinctByteBucket] {
  import DistinctByteBucket._

  def size = separated.size

  //Pad to size and also convert to Seq for equality
  def pad(value: Rec): Seq[Byte] = value.toSeq.padTo(itemSize, 0.toByte)

  def items: List[Rec] = separated.map(_.toArray)

  def insertSingle(value: Rec): Option[DistinctByteBucket] = {
    val pv = pad(value)
    if (!separated.contains(pv)) {
      Some(new DistinctByteBucket(oldSet ++ pv, pv :: separated, itemSize))
    } else {
      None
    }
  }

  def insertBulk(values: Iterable[Rec]): Option[DistinctByteBucket] = {
    val newVals = values.map(pad(_)).filter(!separated.contains(_)).toList.distinct
    if (!newVals.isEmpty) {
      val newData = oldSet ++ newVals.flatten
      Some(new DistinctByteBucket(newData, newVals ::: separated, itemSize))
    } else {
      None
    }
  }

  def pack: Rec = oldSet
}

object PackedSeqBucket {
  val separator: String = "\n"

  def newBucket(kmers: List[String], k: Int) =
    new PackedSeqBucket(Array(), Seq(), k).insertBulk(kmers).get

  class Unpacker(dbLocation: String, minCoverage: Option[Int], buckets: Int)
    extends dbpart.bucketdb.Unpacker[String, PackedSeqBucket] {

    val covDB = new CoverageDB(dbLocation.replace(".kch", "_cov.kch"), buckets)

    def unpack(key: String, value: String, k: Int): PackedSeqBucket = {
      val cov = covDB.get(key).coverages
      new PackedSeqBucket(value.split(separator, -1), cov, k).atMinCoverage(minCoverage)
    }
  }
}

/**
 * A bucket that counts the coverage of each k-mer.
 */
final case class PackedSeqBucket(override val sequences: Array[String],
  override val coverages: Seq[Array[Coverage]], override val k: Int,
  var sequencesUpdated: Boolean = false)
  extends CountingSeqBucket[PackedSeqBucket](sequences, coverages, k) with Bucket[String, PackedSeqBucket] {

  import CountingSeqBucket._
  import PackedSeqBucket._

  def items = sequences

  def pack: String = sequences.mkString(separator)

  def size: Int = sequences.size

  override def insertSingle(value: String): Option[PackedSeqBucket] =
    insertBulk(Seq(value))

  /**
   * Insert a number of k-mers, each with coverage 1.
   */
  override def insertBulk(values: Iterable[String]): Option[PackedSeqBucket] =
    Some(insertBulk(values, values.iterator.map(x => 1)))

  def copy(sequences: Array[String], coverage: Seq[Array[Coverage]],
           sequencesUpdated: Boolean): PackedSeqBucket =
    new PackedSeqBucket(sequences, coverage, k, sequencesUpdated)
}
