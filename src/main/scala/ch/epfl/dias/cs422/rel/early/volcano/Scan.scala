package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, RLEColumn, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  /**
   * Helper function (you do not have to use it or implement it)
   * It's purpose is to show how to convert the [[scannable]] to a
   * specific [[Store]].
   *
   * @param rowId row number (startign from 0)
   * @return the row as a Tuple
   */
  private def getRow(rowId: Int): Tuple = {
    scannable match {
      case rleStore: RLEStore =>
        /**
         * For this project, it's safe to assume scannable will always
         * be a [[RLEStore]].
         */
        ???
    }
  }

  var columns: Seq[IndexedSeq[Tuple]] = IndexedSeq()
  var emittedCount = 0
  val totalColCount: Int = table.getRowType.getFieldCount

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init variables
    columns = IndexedSeq()
    emittedCount = 0

    // read in all columns
    scannable match {
      case rleStore: RLEStore => {
        println(s"totalRowCount = $totalColCount")
        var colCount = 0
        for (i <- 0 until totalColCount) {
          val rleCol = rleStore.getRLEColumn(colCount)
          colCount += 1
          //println(s"rleCol=$rleCol")
          val col = decodeRleCol(rleCol)
          //println(s"col=$col")
          columns = columns :+ col
        }
      }
    }
  }

  def decodeRleCol(rleCol: RLEColumn): IndexedSeq[Tuple] = {

    var col = IndexedSeq[Tuple]()
    for (i <- 0 until rleCol.length) {
      val rleEntry = rleCol(i)
      //println(s"   rleEntry = $rleEntry")
      for (j <- 0 until rleEntry.length.toInt) {
        col = col :+ rleEntry.value
      }
    }
    col
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (columns.isEmpty) {
      return NilTuple
    }
    val totalRowCount = columns(0).length
    if (emittedCount < totalRowCount) {
      // concatnate all columns' rowIdth entry together
      var nextTuple:Tuple = IndexedSeq()
      for (i <- 0 until columns.length) {
        nextTuple = nextTuple ++ columns(i)(emittedCount)
      }
      emittedCount += 1
      Some(nextTuple)
    } else {
      NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
