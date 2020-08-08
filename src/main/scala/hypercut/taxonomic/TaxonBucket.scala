package hypercut.taxonomic

import hypercut.{NTSeq, SequenceID}
import hypercut.hash.{BucketId, ReadSplitter}
import hypercut.spark.{Counting, HashSegment, Routines, SerialRoutines}
import miniasm.genome.bpbuffer.BPBuffer
import miniasm.genome.bpbuffer.BPBuffer.ZeroBPBuffer
import org.apache.spark.sql.SparkSession

import scala.util.Sorting


final class TaxonomicIndex[H](val spark: SparkSession, spl: ReadSplitter[H],
                              taxonomyNodes: String) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import HashSegments._

  val numIndexBuckets = spark.conf.get("spark.sql.shuffle.partitions").toInt
  println(s"Taxonomic index set to $numIndexBuckets buckets")

  import org.apache.spark.sql._
  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  //Broadcasting the splitter mainly because it contains a reference to the MotifSpace,
  //which can be large
  val bcSplit = sc.broadcast(spl)
  val parentMap = getParentMap(taxonomyNodes)
  val bcParentMap = sc.broadcast(parentMap)

  /**
   * Read a taxon label file (TSV format)
   * Maps sequence id to taxon id.
   * This file is expected to be small.
   *
   * @param file
   * @return
   */
  def getTaxonLabels(file: String): Dataset[(String, Taxon)] = {
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

  def taggedToBuckets(segments: Dataset[(BucketId, Array[(ZeroBPBuffer, Taxon)])]): Dataset[TaxonBucket] = {
    val k = spl.k
    val bcPar = this.bcParentMap
    segments.map { case (hash, segments) => {
      TaxonBucket.fromTaggedSequences(hash, bcPar.value.taxonTaggedFromSequences(segments, k).toArray)
    } }
  }

  def segmentsToBuckets(segments: Dataset[(HashSegment, Taxon)]): Dataset[TaxonBucket] = {
    //TODO possible to avoid a shuffle here, e.g. by partitioning preserving transformation?
    val grouped = segments.groupBy($"_1.hash")
    val byHash = grouped.agg(collect_list(struct($"_1.segment", $"_2"))).
      as[(BucketId, Array[(ZeroBPBuffer, Taxon)])]
    taggedToBuckets(byHash)
  }

  def writeBuckets(idsSequences: Dataset[(SequenceID, NTSeq)], seqid2taxidFile: String, output: String): Unit = {
    val bcSplit = this.bcSplit
    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = getTaxonLabels(seqid2taxidFile).toDF("seqId", "taxon")
    val idSeqLabels = idSeqDF.join(broadcast(labels), idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(String, Int)]

    val segments = idSeqLabels.flatMap(r => SerialRoutines.
      createHashSegments(r._1, bcSplit.value).map(s => (s, r._2)))
    val buckets = segmentsToBuckets(segments).
      repartition(numIndexBuckets, $"id")

    /*
     * Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
     * We will reuse the partitioning later when we query the index in the classify operation
     */
    buckets.write.mode(SaveMode.Overwrite).
      option("path", s"${output}_taxidx").
      bucketBy(numIndexBuckets, "id").sortBy("id").
      saveAsTable("taxidx")
  }

  def loadIndexBuckets(location: String): Dataset[TaxonBucket] = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS taxidx")
    spark.sql(s"""|CREATE TABLE taxidx(id long, kmers array<array<long>>, taxa array<int>)
      |USING PARQUET CLUSTERED BY (id) INTO ${numIndexBuckets} BUCKETS
      |LOCATION '${location}_taxidx'
      |""".stripMargin)
    spark.sql("SELECT * FROM taxidx").as[TaxonBucket]
  }

  def classify(indexLocation: String, idsSequences: Dataset[(SequenceID, NTSeq)], k: Int, output: String,
               withUnclassified: Boolean): Unit = {
    val bcSplit = this.bcSplit

    //indexBuckets can potentially be very large, but they are pre-partitioned on disk.
    //Important to avoid shuffling this.
    //Shuffling the subject (idsSequences) being classified should be much easier.
    val indexBuckets = loadIndexBuckets(indexLocation)

    //Segments will be tagged with sequence ID
    val taggedSegments = idsSequences.flatMap(r => createHashSegmentsWithIndex(r._2, r._1, bcSplit.value)).
      //aliasing the hash column before grouping (rather than after) avoids an unnecessary
      // shuffle in the join with indexBuckets further down
      select($"_1.hash".as("id"), $"_1.segment", $"_2", $"_3").
      groupBy("id").agg(collect_list(struct("segment", "_2", "_3")))

    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables
    val subjectWithIndex = taggedSegments.join(indexBuckets, List("id"), "left").
      as[(Long, Array[(ZeroBPBuffer, Int, String)], Array[Array[Long]], Array[Int])]

    val tagsWithLCAs = subjectWithIndex.flatMap(data => {
      val tbkt = if (data._3 != null) TaxonBucket(data._1, data._3, data._4)
        else TaxonBucket(data._1, Array(), Array())

      data._2.map(segment => {
        val ambiguous = (segment._1.data == null)
        if (ambiguous) {
          //hack: segment.size is not BPBuffer length but number of k-mers
          (segment._3, TaxonSummary.ambiguous(segment._2, segment._1.size))
        } else {
          tbkt.classifyKmersBySearch(segment._1, segment._2, segment._3, k)
        }
      })
    })

    val bcPar = this.bcParentMap

    //Group by sequence ID
    val tagLCA = tagsWithLCAs.groupBy("_1").agg(collect_list($"_2")).
      as[(String, Array[TaxonSummary])].flatMap(x => {
      val summariesInOrder = TaxonSummary.concatenate(x._2.sortBy(_.order))

      //Potentially less data to traverse and merge after the order concatenation has been done
      val allHits = TaxonSummary.mergeHitCounts(Seq(summariesInOrder))
      val taxon = bcPar.value.resolveTree(allHits.filter(_._1 != AMBIGUOUS))
      val seqLength = allHits.values.sum + (k - 1)

      if (taxon == ParentMap.NONE && !withUnclassified) {
        None
      } else {
        //Imitate the Kraken output format
        val classifyFlag = if (taxon == ParentMap.NONE) "U" else "C"
        val seqId = x._1
        Some((classifyFlag, seqId, taxon, seqLength, summariesInOrder.toString))
      }
    }).cache

    //materialize to ensure that we compute the table before coalescing it below, or partitions would be too big
    tagLCA.count

    //This table will be relatively small and we coalesce mainly to avoid generating a lot of small files
    //in the case of a fine grained index with many buckets
    tagLCA.coalesce(200).write.mode(SaveMode.Overwrite).option("sep", "\t").
      csv(s"${output}_classified")
    tagLCA.unpersist
  }

  def showIndexStats(location: String): Unit = {
    val idx = loadIndexBuckets(location)
    val stats = idx.map(_.stats)

    stats.cache
    println("Kmer count in buckets: sum " +
      stats.agg(sum("numKmers")).take(1)(0).getLong(0))
    println("Bucket stats:")
    stats.describe().show()
    stats.unpersist
  }
}

object TaxonBucket {
  def fromTaggedSequences(id: BucketId,
                          data: Array[(Array[Long], Taxon)]): TaxonBucket = {
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
                             kmers: Array[Array[Long]],
                             taxa: Array[Taxon]) {

  def stats = TaxonBucketStats(kmers.size, taxa.distinct.size)


  import Counting.LongKmerOrdering

  /**
   * For tagged sequences that belong to this bucket, classify each one using
   * the LCA algorithm. Return a (tag, taxon) pair for each k-mer that has a hit.
   *
   * This version iterates through a potentially large number of subjects by first sorting them,
   * and then iterating together with this bucket (which is already sorted by construction).
   *
   * @param subjects
   * @tparam T
   * @return
   */
  def classifyKmersByIteration[T : Ordering](subjects: Iterable[(BPBuffer, T)], k: Int): Iterator[(T, Taxon)] = {
    implicit val ord = new LongKmerOrdering(k)

    val byKmer = subjects.iterator.flatMap(s =>
      s._1.kmersAsLongArrays(k.toShort).map(km => (km, s._2))
    ).toArray
    Sorting.quickSort(byKmer)

    //Rely on both arrays being sorted
    val bucketIt = kmers.indices.iterator
    val subjectIt = byKmer.iterator
    if (bucketIt.isEmpty || subjectIt.isEmpty) {
      return subjectIt.map(s => (s._2, ParentMap.NONE))
    }

    var bi = bucketIt.next

    val (prePart, remPart) = subjectIt.partition(s =>
      ord.compare(s._1, kmers(bi)) < 0)

    //The same k-mer may occur multiple times in subjects for different tags (but not in the bucket)
    //Need to consider subj again here
    prePart.map(s => (s._2, ParentMap.NONE)) ++ remPart.map(s => {
      while (bucketIt.hasNext && ord.compare(s._1, kmers(bi)) > 0) {
        bi = bucketIt.next
      }
      if (ord.compare(s._1, kmers(bi)) == 0) {
        (s._2, taxa(bi))
      } else {
        (s._2, ParentMap.NONE)
      }
    })
  }

  /**
   * For tagged sequences that belong to this bucket, classify each one using
   * the LCA algorithm. Return a (tag, taxon) pair for each k-mer that has a hit.
   *
   * This version looks up a relatively small number of subjects by binary searching each.
   *
   * @param subject
   * @param order
   * @tparam T
   * @return
   */
  def classifyKmersBySearch[T](subject: BPBuffer, order: Int, tag: T, k: Int): (T, TaxonSummary) = {
    implicit val ordering = new LongKmerOrdering(k)
    val byKmer = subject.kmersAsLongArrays(k.toShort)

    import scala.collection.Searching._
    val raw = byKmer.map(subj => {
      kmers.search(subj) match {
        case Found(f) => taxa(f)
        case _ => ParentMap.NONE
      }
    }).toList
    (tag, TaxonSummary.fromClassifiedKmers(raw, order))
  }
}

final case class TaxonBucketStats(numKmers: Long, distinctTaxa: Long)