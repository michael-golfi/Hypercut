package hypercut.hash

import scala.annotation.tailrec
import scala.collection.{Searching, mutable}
import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.mutable.ArrayBuffer

object FSMScanner {
  val charMod = 5
  val usedCharSet = "ACTGN".toSeq
  //These must all be unique and preferrably small
  val alphabet = usedCharSet.map(x => x % charMod).toArray
}
/**
 * In the 'transitions' array the next state will be looked up through the value of the seen character.
 * There is a match iff features is not None.
 */
final case class ScannerState(val startOffset: Int, features: Option[Features],
                              val transitions: Array[ScannerState]) {
  import FSMScanner._

//  override def toString = s"State[$seenString]"

  def advance(next: Char): ScannerState = {
    transitions(next % charMod)
  }
}

/**
 * A state machine for fast scanning.
 * For convenience, we use the ASCII character directly to index into the state array.
 * This means that some memory will be wasted, since the majority of the ASCII characters do not participate in
 * the tokens we look for (which come from A,C,T,G,N only). However, this is a fixed cost per state and not
 * much of a concern in the big picture.
 */
final class FSMScanner(val space: MotifSpace) {
  import FSMScanner._
  def motifsByPriority = space.byPriority
  def motifs = motifsByPriority.toSet

  val initState = ScannerState(-1, None, alphabet.map(a => null))

  val maxPtnLength = space.width
  def padToLength(ptn: String): Seq[String] = {
    if (ptn.length < maxPtnLength) {
      Seq(ptn + "A", ptn + "C", ptn + "T", ptn + "G").filter(! motifs.contains(_)).
        flatMap(padToLength(_))
    } else {
      Seq(ptn)
    }
  }

  //Construct padded states such as ACT, ACG, ACT from AC when ACA is also a pattern
  //This map will match an extended motif such as ACT to its "true match" AC
  val trueMatches = Map.empty ++ motifsByPriority.flatMap(x => (padToLength(x).map(_ -> x)))

  def hasItemWithPrefix(keys: Seq[String], prefix: String) = {
    Searching.search(keys).search(prefix) match {
      case InsertionPoint(i) =>
        (i < keys.length) &&
          keys(i).startsWith(prefix)
      case Found(i) => true
      case _ => false
    }
  }

  def buildStates() {
    val matchKeys = trueMatches.keys.toSeq.sorted

    val tmpMap = mutable.Map[String, ScannerState]("" -> initState)
    var i = 1
    var strings = usedCharSet.map(_.toString)
    while (i <= maxPtnLength) {
      //Set up the states for seen strings of a certain length
      //Not all will be needed.

      tmpMap ++= strings.map(s => (s -> ScannerState(s.length - 1, trueMatches.get(s).map(m => space.getFeatures(m)),
        alphabet.map(a => initState))))

      for {
        s <- strings;
        a <- usedCharSet
      } {
        //Transition into same length, e.g. AGG into GGT
        tmpMap(s).transitions(a % charMod) = tmpMap(s.substring(1) + a)
        //Transition into longer, e.g. AG into AGG, when a match is possible
        if (hasItemWithPrefix(matchKeys, s)) {
          tmpMap(s.dropRight(1)).transitions(s.last % charMod) = tmpMap(s)
        }
      }
      i += 1
      strings = strings.flatMap(s => usedCharSet.map(a => s + a))
    }
    for {
      a <- usedCharSet
    } {
      //Initial transitions from the init state
      initState.transitions(a % charMod) = tmpMap(a.toString)
    }
  }

  buildStates()

  @tailrec
  def getState(seq: String, from: ScannerState = initState): ScannerState = {
    if (seq.length == 0) {
      from
    } else {
      getState(seq.substring(1), from.advance(seq.charAt(0)))
    }
  }

  /*
    Find all matches in the string.
    Returns an array with the matches in order.
   */
  def allMatches(data: String): ArrayBuffer[Motif] = {
    var state = initState
    var i = 0
    val r = new ArrayBuffer[Motif](data.length)
    while (i < data.length) {
      state = state.advance(data.charAt(i))
      state.features match {
          //exists iff there is a match
        case Some(f) =>
//          println(s"$i $m ${state.seenString} ${state.startOffset}")
          r += Motif(i - state.startOffset, f)
        case _ =>
//          println(s"$i ${state.seenString}")
      }
      i += 1
    }
    r
  }
}
