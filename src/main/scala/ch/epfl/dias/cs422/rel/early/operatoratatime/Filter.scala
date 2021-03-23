package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    var inputCols:IndexedSeq[Column] = IndexedSeq()
    val inputIter = input.iterator
    // read all columns
    while (inputIter.hasNext){
      inputCols = inputCols :+ inputIter.next
    }

    // if input is empty, nothing else to do
    if (inputCols.isEmpty) return inputCols

    // if input is not empty, loop thru each tuple to get the new selection vector
    var newSelectVec:Column = IndexedSeq()
    val numTuple = inputCols(0).length
    val oldSelectVec = inputCols(inputCols.length-1)
    for (i <- 0 until numTuple) {
      // construct a tuple (without the selection boolean)
      var t:Tuple = IndexedSeq()
      for (j <- 0 until inputCols.length-1) {
        t = t :+ inputCols(j)(i)
      }
      // add the reconstructed tuple's selection bit
      // a tuple is active iff it was active and the it passes the predicate
      newSelectVec = newSelectVec :+ (predicate(t) && oldSelectVec(i).asInstanceOf[Boolean])
    }

    // update the selection vector
    inputCols.dropRight(1) :+ newSelectVec
  }
}
