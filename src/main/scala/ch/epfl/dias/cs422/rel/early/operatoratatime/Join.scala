package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {


  def getKeyAsTuple(tuple: Tuple, keyIndices:IndexedSeq[Int]): Tuple = {
    var key = IndexedSeq[Any]()
    for (i <- keyIndices) {
      key = key :+ tuple(i)
    }
    return key
  }

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {

    var allJoinedTuples = IndexedSeq[Tuple]()
    var nextTupleInd = 0
    var leftCols:IndexedSeq[Column] = IndexedSeq()
    var leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, IndexedSeq[Tuple]]

    // read all cols from left
//    var leftIter = left.iterator
//    while (leftIter.hasNext) {
//      leftCols = leftCols :+ leftIter.next()
//    }
    leftCols = left.execute()

    // if there are no rows from left, no more to do
    if (leftCols.isEmpty) return leftCols

    val rowCountL = leftCols(0).length
    val leftTupleLen = leftCols.length - 1 // minus the last col of selection bools
    val oldLeftSelVec = leftCols(leftCols.length-1)
    // if there are some rows, then for active rows, insert tuples to the left hash table
    // so that they are grouped by the left join key
    for (i <- 0 until rowCountL) {
      // only need to insert if this row is active
      if (oldLeftSelVec(i).asInstanceOf[Boolean]) {
        // reconstruct the tuple, to len-2 to exclude the last selection bool
        var nextLeft: Tuple = IndexedSeq()
        for (j <- 0 until leftTupleLen) {
          nextLeft = nextLeft :+ leftCols(j)(i)
        }
        var nextLeftKey = getKeyAsTuple(nextLeft, getLeftKeys)
        // if this is a new key, insert a new (key->Seq(nextLeft))
        if (!leftHashmap.contains(nextLeftKey)) {
          leftHashmap += (nextLeftKey -> (IndexedSeq[Tuple]() :+ nextLeft))
        } else {
          // if this is a existing key, append to corresponding group
          leftHashmap += (nextLeftKey -> (leftHashmap(nextLeftKey) :+ nextLeft))
        }
      }
    }

    // read all columns from right
    var rightCols:IndexedSeq[Column] = IndexedSeq()
//    val rightIter = right.iterator
//    while (rightIter.hasNext) {
//      rightCols = rightCols :+ rightIter.next()
//    }
    rightCols = right.execute()

    // if right is empty, nothing to do
    if (rightCols.isEmpty) return rightCols

    // if rightCols is not empty, for all active rows from right
    // find the right key, find matching left tuples
    // add joined tuples by updating the outputCols
    val rowCountR = rightCols(0).length
    val rightTupleLen = rightCols.length - 1 // minus 1 for last col, the selection vec
    val joinedTupleLen = leftTupleLen + rightTupleLen
    val oldRightSelVec = rightCols(rightCols.length-1)

    // init the outputs array to have empty columns
    var outputCols:Array[Column] = Array()
    for (i <- 0 until joinedTupleLen) {
      outputCols = outputCols :+ IndexedSeq()
    }

    // loop each row of right table
    for (i <- 0 until rowCountR) {
      // only check for matching if this row is active
      if (oldRightSelVec(i).asInstanceOf[Boolean]) {
        // reconstruct the right tuple, to len-2 to exclude the last selection bool
        var nextRight:Tuple = IndexedSeq()
        for (j <- 0 until rightTupleLen) {
          nextRight = nextRight :+ rightCols(j)(i)
        }

        val rightKey = getKeyAsTuple(nextRight, getRightKeys)
        if (leftHashmap.contains(rightKey)) {
          // found matching left key, join with each member in the group
          val matchedLeftTuples = leftHashmap(rightKey)
          for (l <- matchedLeftTuples) {
            var joinedTuple = l ++ nextRight
            //println(s"*** joinedTuple = $joinedTuple")
            //allJoinedTuples = allJoinedTuples :+ joinedTuple
            for (i <- joinedTuple.indices) {
              outputCols(i) = outputCols(i) :+ joinedTuple(i)
            }
          }
        }
      }
    }

    // add selection vector which should all be true
    val outputRowCount = outputCols(0).length
    var newSelVec: Column = IndexedSeq()
    for (i <- 0 until outputRowCount) {
      newSelVec = newSelVec :+ true
    }
    outputCols.toIndexedSeq :+ newSelVec

  }
}
