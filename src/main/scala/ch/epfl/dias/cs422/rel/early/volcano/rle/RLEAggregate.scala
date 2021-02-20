package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
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
