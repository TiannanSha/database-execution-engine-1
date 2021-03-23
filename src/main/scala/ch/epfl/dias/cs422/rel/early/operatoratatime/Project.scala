package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    var inputCols:IndexedSeq[Column] = IndexedSeq()
    var projectedTuples:IndexedSeq[Tuple] = IndexedSeq()
    val inputIter = input.iterator
    // read all columns
    while (inputIter.hasNext){
      inputCols = inputCols :+ inputIter.next
    }

    // if input is empty, nothing else to do
    if (inputCols.isEmpty) return inputCols

    // if input is not empty, project each tuple
    val numTuple = inputCols(0).length
    for (i <- 0 until numTuple) {
      // construct a tuple (without the selection boolean)
      var t:Tuple = IndexedSeq()
      for (j <- 0 until inputCols.length-1) {
        t = t :+ inputCols(j)(i)
      }
      // project the reconstructed tuple
      projectedTuples = projectedTuples :+ evaluator(t)
      //val projected = evaluator(t)
    }

    // if there's no tuple, map each input col to an empty col
    if (projectedTuples.isEmpty) return inputCols.map(col => IndexedSeq())

    // if there are some tuples, add each column of projected tuples
    val numProjectedCol = projectedTuples(0).length
    var outputCols:IndexedSeq[Column] = IndexedSeq()
    // add column one by one
    for (i <- 0 until numProjectedCol) {
      var col:Column = IndexedSeq()
      for (j <- projectedTuples.indices) {
        val t = projectedTuples(j)
        col = col :+ t(i)
      }
      outputCols = outputCols :+ col
    }
    println(s"projectedTuples = $projectedTuples")
    println(s"In projection, outputCols = $outputCols")
    // add the selection vector
    outputCols :+ inputCols(inputCols.length-1)
  }
}
