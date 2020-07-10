package hypercut.taxonomic

import hypercut.hash.{BucketId, ReadSplitter}
import hypercut.spark.{Counting, HashSegment, Routines}
import hypercut.spark.SerialRoutines._
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Sorting


final class TaxonomicIndex[H](val spark: SparkSession, spl: ReadSplitter[H],
                              taxonomyNodes: String) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  val routines = new Routines(spark)

  val indexBuckets = 400

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._
  import Counting._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)
  val parentMap = getParentMap(taxonomyNodes)
  val bcParentMap = sc.broadcast(parentMap)


  /**
   * Read a taxon label file (TSV format)
   * This file is expected to be small.
   *
   * @param file
   * @return
   */
  def getTaxonLabels(file: String): Dataset[(String, Int)] = {
    spark.read.option("sep", "\t").csv(file).
      map(x => (x.getString(0), x.getString(1).toInt))
  }

  /**
   * Read an ancestor map from an NCBI nodes.dmp style file.
   * This file is expected to be small.
   *
   * @param file
   * @return
   */
  def getParentMap(file: String): ParentMap = {
    val raw = spark.read.option("sep", "|").csv(file).repartition(100).
      map(x => (x.getString(0).trim.toInt, x.getString(1).trim.toInt)).collect()
    val maxTaxon = raw.map(_._1).max
    val asMap = raw.toMap
    val asArray = (0 to maxTaxon).map(x => asMap.getOrElse(x, ParentMap.NONE)).toArray
    asArray(1) = ParentMap.NONE //1 is root of tree
    new ParentMap(asArray)
  }

  def taggedToBuckets(segments: Dataset[(BucketId, Array[(ZeroBPBuffer, Int)])]): Dataset[TaxonBucket] = {
    val k = spl.k
    val bcPar = this.bcParentMap
    segments.map { case (hash, segments) => {
      TaxonBucket.fromTaggedSequences(hash, bcPar.value.taxonTaggedFromSequences(segments, k).toArray)
    }
    }
  }

  def segmentsToBuckets(segments: Dataset[(HashSegment, Int)]): Dataset[TaxonBucket] = {
    val grouped = segments.groupBy($"_1.hash")
    val byHash = grouped.agg(collect_list(struct($"_1.segment", $"_2"))).
      as[(BucketId, Array[(ZeroBPBuffer, Int)])]
    taggedToBuckets(byHash)
  }

  def writeBuckets(idsSequences: Dataset[(String, String)], labelFile: String, output: String): Unit = {
    val bcSplit = this.bcSplit
    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = getTaxonLabels(labelFile).toDF("seqId", "taxon")
    val joined = idSeqDF.join(broadcast(labels), idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(String, Int)]

    val segments = joined.flatMap(r => createHashSegments(r._1, r._2, bcSplit.value))
    val buckets = segmentsToBuckets(segments).
      repartition(indexBuckets, $"id")

    /*
     * Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning
     */
    buckets.write.mode(SaveMode.Overwrite).
      option("path", s"${output}_taxidx").
      bucketBy(indexBuckets, "id").sortBy("id").
      saveAsTable("taxidx")
  }

  def classify(indexLocation: String, idsSequences: Dataset[(String, String)], k: Int, output: String): Unit = {
    val bcSplit = this.bcSplit
    val tbuckets = spark.sql("SELECT * FROM taxidx").as[TaxonBucket]
    val idSeqDF = idsSequences.toDF("seqId", "seq").as[(String, String)]

    //Segments will be tagged with sequence ID
    val taggedSegments = idSeqDF.flatMap(r => createHashSegments(r._2, r._1, bcSplit.value)).
      groupByKey(_._1.hash)

    val classified = taggedSegments.cogroup(tbuckets.groupByKey(_.id))((bucketId, segmentsIt, idxBucketIt) => {
      if (idxBucketIt.isEmpty || segmentsIt.isEmpty) {
        Iterator.empty
      } else {
        //only one bucket per key
        val idxBkt = idxBucketIt.next
        val bkt = TaxonBucket(bucketId, idxBkt.kmers, idxBkt.taxa)
        //TODO exploit sort order of segmentsIt if possible
        bkt.classifyKmers(segmentsIt.map(x => (x._1.segment, x._2)).toList, k)
      }
    })

    val bcPar = this.bcParentMap
    //Group by sequence ID
    val tagLCA = classified.groupBy("_1").agg(collect_list($"_2")).
      as[(String, Array[Int])].map(x => (x._1, bcPar.value.classifySequence(x._2)))

    //TODO resolve_tree style algorithm
    tagLCA.write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${output}_classified")
  }
}

object TaxonBucket {
  def fromTaggedSequences(id: BucketId,
                          data: Array[(Array[Int], Int)]): TaxonBucket = {
    val kmers = data.map(_._1)
    val taxa = data.map(_._2)
    TaxonBucket(id, kmers, taxa)
  }
}

/**
 * A bucket where each k-mer is tagged with a taxon.
 * K-mers are sorted.
 *
 * @param id
 * @param kmers
 * @param taxa
 */
final case class TaxonBucket(id: BucketId,
                             kmers: Array[Array[Int]],
                             taxa: Array[Int]) {

  implicit def ordering[T] = Counting.tagOrdering[T]

  import Counting.KmerOrdering

  /**
   * For tagged sequences that belong to this bucket, classify each one using
   * the LCA algorithm. Return a (tag, taxon) pair for each k-mer that has a hit.
   *
   * @param data
   * @tparam T
   * @return
   */
  def classifyKmers[T](subjects: Iterable[(BPBuffer, T)], k: Int): Iterator[(T, Int)] = {
    val byKmer = subjects.iterator.flatMap(s =>
      s._1.kmersAsArrays(k.toShort).map(km => (km, s._2))
    ).toArray
    Sorting.quickSort(byKmer)

    //Rely on both arrays being sorted
    val bucketIt = kmers.indices.iterator
    val subjectIt = byKmer.iterator
    if (bucketIt.isEmpty || subjectIt.isEmpty) {
      return Iterator.empty
    }
    var bi = bucketIt.next
    var subj = subjectIt.next
    while (subjectIt.hasNext && KmerOrdering.compare(subj._1, kmers(bi)) < 0) {
      subj = subjectIt.next
    }

    //The same k-mer may occur multiple times in subjects for different tags (but not in the bucket)
    //Need to consider subj again here
    (Iterator(subj) ++ subjectIt).flatMap(s => {
      while (bucketIt.hasNext && KmerOrdering.compare(s._1, kmers(bi)) > 0) {
        bi = bucketIt.next
      }
      if (KmerOrdering.compare(s._1, kmers(bi)) == 0) {
        Some((s._2, taxa(bi)))
      } else {
        None
      }
    })
  }

  def merge(other: TaxonBucket, ancestors: ParentMap): TaxonBucket = {
    ???
  }
}