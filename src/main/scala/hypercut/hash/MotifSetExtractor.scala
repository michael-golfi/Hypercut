package hypercut.hash

import scala.annotation.tailrec
import hypercut._
import hypercut.shortread.ReadFiles
import hypercut.shortread.Read

import scala.collection.mutable.ArrayBuffer


/**
 * Split a read by motif sets, optionally also using distances between motifs.
 */
final case class MotifSetExtractor(space: MotifSpace, k: Int,
                                   distances: Boolean = true) extends ReadSplitter[MotifSet] {
  @transient
  lazy val scanner = new ShiftScanner(space)

  @volatile
  var readCount = 0

  val n = space.n

  /**
   * Look for motif sets in a read.
   * Returns two arrays:
   * 1. The MotifSet of every k-mer (in order),
   * 2. The positions where each contiguous MotifSet region is first detected
   */
  def motifSetsInRead(read: NTSeq): (ArrayBuffer[MotifSet], ArrayBuffer[(MotifSet, Int)]) = {
    readCount += 1
    if (readCount % 100000 == 0) {
      println(s"$readCount reads seen")
    }

    if (read.length < k) {
      return (ArrayBuffer.empty, ArrayBuffer.empty)
    }

    val perPosition = new ArrayBuffer[MotifSet](read.length)
    val perBucket = new ArrayBuffer[(MotifSet, Int)](read.length)

    val ext = new WindowExtractor(space, scanner, TopRankCache(n), k, read)
    ext.scanTo(k - 2)
    var p = k - 1

    var lastMotifs: List[Motif] = null
    var lastMotifSet: MotifSet = null

    while (p <= read.length - 1) {
      val scan = ext.scanTo(p)
      if (!(scan eq lastMotifs)) {
        if (distances) {
          lastMotifSet = new MotifSet(space, MotifSet.relativePositionsSortedFirstZero(space, scan))
        } else {
          //TODO this can probably be made more efficient by not generating motifs with positions in the first place
          lastMotifSet = new MotifSet(space, scan.iterator.map(_.copy(pos = 0)).toList)
        }
        lastMotifs = scan
        perBucket += ((lastMotifSet, p))
      }
      perPosition += lastMotifSet
      p += 1
    }
    (perPosition, perBucket)
  }

  def split(read: NTSeq): Iterator[(MotifSet, NTSeq)] = {
    val bkts = motifSetsInRead(read)._2.toList
    SplitterUtils.splitRead(k, read, bkts).iterator
  }

  /**
   * Ingest a read.
   * Returns the sequence of discovered buckets, as well as the k-mers in the read.
   * k-mers and buckets at the same position will correspond to each other.
   */
  def motifs(read: NTSeq): (List[MotifSet], Iterator[NTSeq]) = {
    val kmers = Read.kmers(read, k)
    val mss = motifSetsInRead(read)
    (mss._1.toList, kmers)
  }

  /**
   * Ingest a read, returning pairs of discovered buckets (in compact form)
   * and corresponding k-mers.
   */
  def compactMotifs(read: NTSeq): List[(CompactNode, NTSeq)] = {
    val kmers = Read.kmers(read, k).toList
    val mss = motifSetsInRead(read)._1.map(_.compact).toList
    mss zip kmers
  }

  def prettyPrintMotifs(input: NTSeq) = {
    val data = ReadFiles.iterator(input)
    for (read <- data) {
      print(s"Read: $read")
      val analysed = motifs(read)
      var lastMotifs: String = ""
      for ((motifs, kmer) <- (analysed._1 zip analysed._2.toList)) {
        if (motifs.packedString == lastMotifs) {
          print(s"$kmer ")
        } else {
          println("")
          lastMotifs = motifs.packedString
          println(s"  $lastMotifs")
          print(s"    $kmer ")
        }
      }

      println("  Edges ")
      MotifSetExtractor.visitTransitions(analysed._1, (e, f) => print(
        e.packedString + " -> " + f.packedString + " "))
      println("")
    }
  }

  def compact(hash: MotifSet) = hash.compactLong(distances)
}

object MotifSetExtractor {

  def fromSpace(spaceName: String, numMotifs: Int, k: Int) = {
    val space = MotifSpace.named(spaceName, numMotifs)
    new MotifSetExtractor(space, k)
  }

  /**
   * Efficiently visit bucket transitions (i.e. macro edges) in a list that was previously
   * computed by motifSetsInRead.
   */
  @tailrec
  def visitTransitions(data: List[MotifSet], f: (MotifSet, MotifSet) => Unit) {
    data match {
      case x :: y :: xs =>
        if ((x eq y) || (x.compact == y.compact)) {
          visitTransitions(y :: xs, f)
        } else {
          f(x, y)
          visitTransitions(y :: xs, f)
        }
      case _ =>
    }
  }

  @tailrec
  def collectTransitions[A](data: List[A], acc: List[(A, A)] = Nil): List[(A, A)] = {
    data match {
      case x :: y :: xs =>
        collectTransitions(y :: xs, (x, y) :: acc)
      case _ => acc
    }
  }
}