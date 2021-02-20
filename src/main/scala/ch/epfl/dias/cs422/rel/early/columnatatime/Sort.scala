package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = ???
}
