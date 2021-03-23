package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  val numCol = table.getRowType.getFieldCount
  val numRow = scannable.getRowCount.toInt
  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    var outputs:IndexedSeq[Column] = IndexedSeq()
    // add all attribute columns
    for (i <- 0 until numCol) {
      val col:Column = scannable.getColumn(i).toIndexedSeq
      outputs = outputs :+ col
    }
    // add selection vector column
    var svCol:Column = IndexedSeq()
    for (i <- 0 until numRow) {
      svCol = svCol :+ true
    }
    outputs :+ svCol
  }
}
