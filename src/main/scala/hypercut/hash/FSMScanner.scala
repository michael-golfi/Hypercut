package hypercut.hash

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * In the 'transitions' array the next state will be looked up through the value of the seen character.
 */
final case class ScannerState(val seenString: String, foundMotif: Option[String] = None,
                              features: Option[Features] = None,
                              var transitions: Array[ScannerState]) {

  //TODO populate this state with the found code pattern (etc) for each found motif, to avoid lookups later

  val startOffset = (seenString.length - 1)

  override def toString = s"State[$seenString]"

  def advance(next: Char): ScannerState = {
    transitions(next)
  }

  def isTerminal(init: ScannerState) = !transitions.exists(x => !(x eq init))
}

/**
 * A state machine for fast scanning.
 * For convenience, we use the ASCII character directly to index into the state array.
 * This means that some memory will be wasted, since the majority of the ASCII characters do not participate in
 * the tokens we look for (which come from A,C,T,G,N only). However, this is a fixed cost per state and not
 * much of a concern in the big picture.
 */
final class FSMScanner(val space: MotifSpace) {
  def motifsByPriority = space.byPriority

  val alphabet = (0.toChar to 'T').toArray //includes ACTG
  val initState = ScannerState("", None, None, alphabet.map(a => null))
  val usedCharSet = Seq('A', 'C', 'T', 'G', 'N')

  val maxPtnLength = motifsByPriority.map(_.length).max
  def padToLength(ptn: String): Seq[String] = {
    if (ptn.length < maxPtnLength) {
      Seq(ptn + "A", ptn + "C", ptn + "T", ptn + "G").filter(! motifsByPriority.contains(_)).
        flatMap(padToLength(_))
    } else {
      Seq(ptn)
    }
  }

  //Construct padded states such as ACT, ACG, ACT from AC when ACA is also a pattern
  //This map will match an extended motif such as ACT to its "true match" AC
  val trueMatches = Map() ++ motifsByPriority.flatMap(x => (padToLength(x).map(_ -> x)))

  def buildStatesFrom(seen: String, filtered: Seq[String]): Array[ScannerState] = {
    val candidates = filtered.filter(_.startsWith(seen))
    val i = seen.length()
    alphabet.map(a => {
      if (candidates.exists(_.startsWith(seen + a))) {
        val next = seen + a
        val (matched, features) = {
          if (filtered.contains(next)) {
            val trueMatch = trueMatches(next)
            (Some(trueMatch), Some(space.getFeatures(trueMatch)))
          } else {
            (None, None)
          }
        }

        ScannerState(next, matched, features,
          buildStatesFrom(next, filtered.filter(_.startsWith(next)))
        )
      } else initState
    }).toArray
  }

  def buildStates() {
    val matchKeys = trueMatches.keys.toSeq
    var tmpMap = Map[String, ScannerState]("" -> initState)
    var i = 1
    var strings = usedCharSet.map(_.toString)
    while (i <= maxPtnLength) {
      //Set up the states for seen strings of a certain length
      //Not all will be needed.
      tmpMap ++= strings.map(s => (s -> ScannerState(s, trueMatches.get(s),
        trueMatches.get(s).map(m => space.getFeatures(m)),
        alphabet.map(a => initState))))

      for {
        s <- strings;
        a <- usedCharSet
      } {
        //Transition into same length, e.g. AGG into GGT
        tmpMap(s).transitions(a) = tmpMap(s.substring(1) + a)
        //Transition into longer, e.g. AG into AGG, when a match is possible
        if (matchKeys.exists(_.startsWith(s))) {
          tmpMap(s.dropRight(1)).transitions(s.last) = tmpMap(s)
        }
      }
      i += 1
      strings = strings.flatMap(s => usedCharSet.map(a => s + a))
    }
    for {
      a <- usedCharSet
    } {
      //Initial transitions from the init state
      initState.transitions(a) = tmpMap(a.toString)
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
