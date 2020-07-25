package hypercut.spark


object Kmer {
  /**
   * Construct a k-mer from data obtained using a BPBuffer.
   * @param k
   * @param data
   */
  def apply(data: Array[Int]): Kmer = {
    if (data.length == 2) {
      Kmer2(data(0), data(1))
    } else if (data.length == 3) {
      Kmer3(data(1), data(2), data(3))
    } else {
      ???
    }
  }
}
/**
 * Highly compact k-mer representation for spark usage.
 */
trait Kmer {

}

final case class Kmer2(data1: Int, data2: Int) extends Kmer
final case class Kmer3(data1: Int, data2: Int, data3: Int) extends Kmer
