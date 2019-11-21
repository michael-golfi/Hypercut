package hypercut.hash

import scala.annotation.tailrec

/**
 * In the 'transitions' array the next state will be looked up through the value of the seen character.
 */
final case class ScannerState(val seenString: String, foundMarker: Option[String] = None,
  var transitions: Array[ScannerState]) {

  val startOffset = (seenString.length - 1)

  override def toString = s"State[$seenString]"

  def advance(next: Char): ScannerState = {
    transitions(next)
  }

  def isTerminal(init: ScannerState) = !transitions.exists(x => !(x eq init))
}

/**
 * Builds a state machine for fast parsing.
 * We expect to match on short markers, so we are not too worried about wasted memory
 * (most of the positions in the transitions arrays will be unused).
 */
final class FSMScanner(val markersByPriority: Seq[String]) {
  val alphabet = (0.toChar to 'T').toArray //includes ACTG
  val initState = ScannerState("", None, alphabet.map(a => null))
  val usedCharSet = Seq('A', 'C', 'T', 'G', 'N')

  val maxPtnLength = markersByPriority.map(_.length).max
  def padToLength(ptn: String): Seq[String] = {
    if (ptn.length < maxPtnLength) {
      Seq(ptn + "A", ptn + "C", ptn + "T", ptn + "G").filter(! markersByPriority.contains(_)).
        flatMap(padToLength(_))
    } else {
      Seq(ptn)
    }
  }

  //Construct padded states such as ACT, ACG, ACT from AC when ACA is also a pattern
  //This map will match an extended marker such as ACT to its "true match" AC
  val trueMatches = Map() ++ markersByPriority.flatMap(x => (padToLength(x).map(_ -> x)))

  def buildStatesFrom(seen: String, filtered: Seq[String]): Array[ScannerState] = {
    val candidates = filtered.filter(_.startsWith(seen))
    val i = seen.length()
    alphabet.map(a => {
      if (candidates.exists(_.startsWith(seen + a))) {
        val next = seen + a
        val matched = if (filtered.contains(next)) Some(trueMatches(next)) else None
        ScannerState(next, matched,
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

  def allMatches(data: String) = {
    var state = initState
    var i = 0
    var r = List[(Int, String)]()
    while (i < data.length) {
      state = state.advance(data.charAt(i))
      state.foundMarker match {
        case Some(m) =>
//          println(s"$i $m ${state.seenString} ${state.startOffset}")
          r ::= (((i - state.startOffset), m))
        case _ =>
//          println(s"$i ${state.seenString}")
      }
      i += 1
    }
    r
  }
}
