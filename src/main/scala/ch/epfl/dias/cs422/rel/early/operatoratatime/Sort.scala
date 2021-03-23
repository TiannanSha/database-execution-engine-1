package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._
import scala.math.Ordered.orderingToOrdered

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  // define ordering for tuple
  object TupleOrdering extends Ordering[Tuple] {
    def compare(t1:Tuple, t2:Tuple): Int = {
      //var keysIter = collation.getKeys.iterator()
      var result = 0

      // e.g. field collation = [0 DESC, 2, 1, 3]
      val fieldCollIter = collation.getFieldCollations.iterator()
      while (fieldCollIter.hasNext) {
        var fieldColl = fieldCollIter.next()
        var fieldIndex = fieldColl.getFieldIndex
        if (fieldColl.direction.isDescending) {
          result = t1(fieldIndex).asInstanceOf[Comparable[Elem]] compare
            t2(fieldIndex).asInstanceOf[Comparable[Elem]]
        } else {
          result = t2(fieldIndex).asInstanceOf[Comparable[Elem]] compare
            t1(fieldIndex).asInstanceOf[Comparable[Elem]]
        }
        if (result!=0) {
          // we have decided a winner
          return result
        }
      }
      // all keys are the same, no winner
      return 0
    }
  }

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    // read all input cols
    var inputCols:IndexedSeq[Column] = IndexedSeq()
    val iter = input.iterator
    while (iter.hasNext) {
      inputCols = inputCols :+ iter.next()
    }

    // if input is empty, nothing to do
    if (inputCols.isEmpty) return inputCols

    // reconstruct each active tuple and insert into the priority queue
    val numTuple = inputCols(0).length
    var pq = collection.mutable.PriorityQueue[Tuple]()(TupleOrdering)
    val oldSelectVec = inputCols(inputCols.length-1)
    for (i <- 0 until numTuple) {
      if (oldSelectVec(i).asInstanceOf[Boolean]) {
        // construct an active tuple (without the selection boolean)
        var t: Tuple = IndexedSeq()
        for (j <- 0 until inputCols.length - 1) {
          t = t :+ inputCols(j)(i)
        }

        // insert constructed tuple into priority queue
        pq.enqueue(t)
      }
    }

    // extract required number of best tuples or all tuples by
    // selecting top *nTop* tuples and then drop *nDrop* tuples
    var nDrop = 0
    var nTop = 0
    if (offset.isEmpty) {
      nDrop = 0
    } else {
      nDrop = offset.get
    }

    if (fetch.isDefined) {
      // when fetch is specified, take the min(pq.size, fetch) number of tuples
      nTop = nDrop + Math.min(fetch.get, pq.size)
    } else {
      // when not specifying fetch, want all available tuples
      nTop = pq.size
    }

    // init outputCols with empty columns
    var outputCols:Array[Column] = Array()
    for (i <- inputCols.indices) {
      outputCols = outputCols :+ IndexedSeq()
    }

    for (i <- 1 to nTop) {
      //outputTuples = outputTuples :+ pq.dequeue()
      // add each tuple by updating each col
      val t:Tuple = pq.dequeue()
      for (j <- t.indices) {
        outputCols(j) = outputCols(j) :+ t(j)
      }
      outputCols(outputCols.length-1) = outputCols(outputCols.length-1) :+ true
    }
    outputCols.toIndexedSeq
  }
}
