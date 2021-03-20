package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  var inputTuples:IndexedSeq[Tuple] = IndexedSeq()
  var emittedCount = 0

  def RleEntryToTuples(rleEntry: RLEentry): IndexedSeq[Tuple] = {
    var tuples: IndexedSeq[Tuple] = IndexedSeq()
    for (i <- 0 until rleEntry.length.toInt ) {
      tuples = tuples :+ rleEntry.value
    }
    tuples
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init vars
    inputTuples = IndexedSeq()
    emittedCount = 0

    // read all inputs
    val iter = input.iterator
    while (iter.hasNext) {
      val nextInput = iter.next()
      inputTuples = inputTuples ++ RleEntryToTuples(nextInput)
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (emittedCount < inputTuples.length ) {
      val output = Some(inputTuples(emittedCount))
      emittedCount += 1
      output
    } else {
      NilTuple
    }

  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
