package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
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

  //val leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, Tuple]
  var allJoinedRLEs = IndexedSeq[RLEentry]()
  var nextTupleInd = 0

  def getKeyAsTuple(tuple: Tuple, keyIndices:IndexedSeq[Int]): Tuple = {
    var key = IndexedSeq[Any]()
    for (i <- keyIndices) {
      key = key :+ tuple(i)
    }
    return key
  }

  def joinTwoRLE(rle1:RLEentry, rle2:RLEentry): RLEentry = {
    RLEentry(rle1.startVID,
      scala.math.min(rle1.length, rle2.length),
      rle1.value++rle2.value)
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    // init variables
    var leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, IndexedSeq[RLEentry]]
    nextTupleInd = 0
    allJoinedRLEs = IndexedSeq[RLEentry]()
    //var leftTuples = IndexedSeq[Tuple]()

    // store all tuples from left table to the hash table
    // so that they are grouped by the left join key
    var leftIter = left.iterator
    //var leftCount = 0
    while (leftIter.hasNext) {
      var nextLeft = leftIter.next()
      var nextLeftKey = getKeyAsTuple(nextLeft.value, getLeftKeys)

      // if this is a new key, insert a new (key->Seq(nextLeft))
      if (!leftHashmap.contains(nextLeftKey)) {
        leftHashmap += (nextLeftKey -> (IndexedSeq[RLEentry]() :+ nextLeft))
        //leftCount += 1
      } else {
        // if this is a existing key, append to corresponding group
        leftHashmap += (  nextLeftKey -> (leftHashmap(nextLeftKey) :+ nextLeft)  )
        //leftCount += 1
      }
    }

    // read tuple from right table one by one and do the join by
    // concatnating with all tuples in the bucket with same key
    var rightIter = right.iterator
    //var rightCount = 0
    //var rightTuples = IndexedSeq[Tuple]()
    while (rightIter.hasNext) {
      //rightCount += 1
      var nextRight = rightIter.next()
      //rightTuples = rightTuples :+ nextRight
      val rightKey = getKeyAsTuple(nextRight.value, getRightKeys)
      if (leftHashmap.contains(rightKey)) {
        // found matching left key, join with each member in the group
        val matchedLeftRLEs = leftHashmap(rightKey)
        for (l <- matchedLeftRLEs) {
          var joinedRLE = joinTwoRLE(l, nextRight)
          allJoinedRLEs = allJoinedRLEs :+ joinedRLE
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (nextTupleInd < allJoinedRLEs.length) {
      val nextTuple = Option(allJoinedRLEs(nextTupleInd))
      nextTupleInd += 1
      return nextTuple
    } else {
      return NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

  }
}
