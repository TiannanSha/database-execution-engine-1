package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Functions that each of takes the input columns (without selection vec) and produces
    * a column of all projected tuples
    */
  lazy val evals: IndexedSeq[IndexedSeq[HomogeneousColumn] => HomogeneousColumn] =
    projects.asScala.map(e => map(e, input.getRowType, isFilterCondition = false)).toIndexedSeq

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    val inputCols: IndexedSeq[HomogeneousColumn] = input.execute
    val inputColsNoSV = inputCols.dropRight(1)  // exclude the selection vector
    val oldSelVec = inputCols(inputCols.length-1)
    var outputCols: IndexedSeq[HomogeneousColumn] = IndexedSeq()
    for (e <- evals) {
      outputCols = outputCols :+ e(inputColsNoSV)
    }
    outputCols :+ oldSelVec
  }
}
