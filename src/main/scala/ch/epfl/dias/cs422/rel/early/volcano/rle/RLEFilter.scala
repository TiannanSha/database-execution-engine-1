package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEFilter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  var outputs:IndexedSeq[RLEentry] = IndexedSeq()
  var emittedCount = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init vars
    outputs = IndexedSeq()
    emittedCount = 0

    // read all inputs
    val iter = input.iterator
    while (iter.hasNext) {
      val nextRLE = iter.next()
      if (predicate(nextRLE.value)){
        outputs = outputs :+ nextRLE
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (emittedCount < outputs.length) {
      val output = outputs(emittedCount)
      emittedCount += 1
      Some(output)
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

  }
}
