package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  // get the key (which is a smaller tuple) of a tuple
  def getKey(t: Tuple): IndexedSeq[Any] = {
    var key = IndexedSeq[Any]()
    //var groupList = groupSet
    var groupSetIter = groupSet.iterator()
    while (groupSetIter.hasNext){
      val nextK = groupSetIter.next()
      key = key :+ t(nextK.toInt)
    }
    return key
  }

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
//    var inputCols:IndexedSeq[Column] = IndexedSeq()
//    val inputIter = input.iterator
//    // read all columns
//    while (inputIter.hasNext){
//      inputCols = inputCols :+ inputIter.next
//    }
    val inputCols = input.execute()

    // if input is empty, nothing else to do
    if (inputCols.isEmpty) return inputCols

    // if input is not empty, reconstruct each active tuple and insert into its group
    val numTuple = inputCols(0).length
    var inputTuplesGrouped = Map[IndexedSeq[Any], IndexedSeq[Tuple]]()
    val oldSelectVec = inputCols(inputCols.length-1)

    for (i <- 0 until numTuple) {
      if (oldSelectVec(i).asInstanceOf[Boolean]) {
        // construct an active tuple (without the selection boolean)
        // and then insert into its group
        var t: Tuple = IndexedSeq()
        for (j <- 0 until inputCols.length - 1) {
          t = t :+ inputCols(j)(i)
        }

        // insert t to its group
        var k = getKey(t)
        // if key already exists, append to the list
        // if key is new, add (key, listOfTuple) to the dictionary
        var groupK = inputTuplesGrouped.get(k)
        if (groupK.isEmpty)
          inputTuplesGrouped += (k -> IndexedSeq(t))
        else {
          inputTuplesGrouped += (k -> groupK.get.appended(t))
        }
      }
    }

    var outputCols:Array[Column] = Array()
    // if all groups are empty and groupby clause is empty, return empty value for each agg
    if (inputTuplesGrouped.isEmpty && groupSet.isEmpty) {
      // init outputcols with empty columns
      for (i <- aggCalls.indices) {
        outputCols = outputCols :+ IndexedSeq()
      }
      // insert output tuples by updating col by col
      var aggedTuple:Tuple = IndexedSeq()
      for (i <- aggCalls.indices) {
        val agg = aggCalls(i)
        outputCols(i) = outputCols(i) :+ agg.emptyValue
        //aggedTuple = aggedTuple :+ agg.emptyValue
      }
      // add the selection vector, which is Vec(true)
      outputCols = outputCols :+ IndexedSeq(true)
      return outputCols.toIndexedSeq
    }

    // aggregate each non-empty groups
    // init outputCols
    outputCols = Array()

    // init outputCols with (keyLen + aggedValCount + 1) empty columns
    val lenAggedTuple:Int =
      inputTuplesGrouped.keys.head.length + aggCalls.length
    for (i <- 0 until lenAggedTuple) {
      outputCols = outputCols :+ IndexedSeq()
    }
    outputCols = outputCols :+ IndexedSeq() // add the col as selection vec

    for (key <- inputTuplesGrouped.keys) {
      var group = inputTuplesGrouped(key)
      var aggedTuple:Tuple = IndexedSeq()
      // add keys first
      aggedTuple = aggedTuple ++ key.asInstanceOf[Tuple]
      // for each aggregation, map group to agg args and reduce to get the final aggregated value
      // append all the aggregated values to get final aggregated tuple for a group
      for (agg <- aggCalls) {
        var mappedGroup = group.map(tuple => agg.getArgument(tuple))
        var aggedVal = mappedGroup.reduce(agg.reduce)
        aggedTuple = aggedTuple :+ aggedVal
      }

      // insert aggedTuple into outputCols by updating each col
      for (i <- aggedTuple.indices) {
        outputCols(i) = outputCols(i) :+ aggedTuple(i)
      }
      // add a true to the last col
      outputCols(outputCols.length-1) = outputCols(outputCols.length-1) :+ true
    }

    outputCols.toIndexedSeq
  }
}
