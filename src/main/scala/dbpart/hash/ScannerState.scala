package dbpart.hash

import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import miniasm.genome.util.DNAHelpers

/**
 * In the 'following' array the next state will be looked up through the value of the seen character.
 */
final case class ScannerState(val seenString: String, foundMarker: Option[String] = None,
  var transitions: Array[ScannerState]) {

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
final class ScannerFSM(val markersByPriority: Seq[String]) {
  val initState = ScannerState("", None, Array())
  val alphabet = (0.toChar to 'T').toArray //includes ACTG

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

  initState.transitions = buildStatesFrom("", trueMatches.keys.toSeq)

  def propagateTransitions(from: ScannerState, seen: String, visitedStrings: Set[String] = Set()) {
    if (from.isTerminal(initState) && !(from eq initState) && seen.length > 1) {
      from.transitions = getState(seen.substring(1)).transitions
    }
    for (a <- alphabet) {
      //Check string length to avoid infinite loop
      if (from.transitions(a) != initState && from.transitions(a).seenString.length() > seen.length()) {
        propagateTransitions(from.transitions(a), seen + a)
      }
    }
  }

  propagateTransitions(initState, "")

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
        case Some(m) => r ::= (((i - m.length() + 1), m))
        case _ =>
      }
      i += 1
    }
    r
  }
}
