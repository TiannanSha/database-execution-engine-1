package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * @inheritdoc
    */
  override def open(): Unit = ???

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = ???

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
