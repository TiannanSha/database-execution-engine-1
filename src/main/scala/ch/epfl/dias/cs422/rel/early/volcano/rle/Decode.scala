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

  /**
    * @inheritdoc
    */
  override def open(): Unit = ???

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = ???

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
