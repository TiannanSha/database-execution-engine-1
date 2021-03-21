package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
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

  // state variables
  //var inputTuplesGrouped = Map[Tuple, IndexedSeq[Tuple]]()
  var nextTupleInd : Int = 0
  var allAggedRLEs = IndexedSeq[RLEentry]() // each entry is the result from aggregating a group

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
  override def open(): Unit = {

    // init variables
    allAggedRLEs = IndexedSeq()
    var inputTuplesGrouped = Map[IndexedSeq[Any], IndexedSeq[RLEentry]]()
    nextTupleInd = 0

    // read each tuple into appropriate group
    var inputIter = input.iterator
    var inputCount = 0
    while(inputIter.hasNext) {
      inputCount+=1
      var nextInput = inputIter.next()
      var k = getKey(nextInput.value)

      // insert into the dictionary
      // if key already exists, append to the list
      // if key is new, add (key, listOfTuple) to the dictionary
      var groupK = inputTuplesGrouped.get(k)
      if (groupK.isEmpty)
        inputTuplesGrouped += (k->IndexedSeq(nextInput))
      else {
        inputTuplesGrouped += (k->groupK.get.appended(nextInput))
      }
    }

    // if all groups are empty and groupby clause is empty, return empty value for each agg
    if (inputTuplesGrouped.isEmpty && groupSet.isEmpty) {
      //println(s"groupSet.isEmpty = ${groupSet.isEmpty}")
      var aggedTuple:Tuple = IndexedSeq()
      for (agg <- aggCalls) {
        aggedTuple = aggedTuple :+ agg.emptyValue
      }
      allAggedRLEs = allAggedRLEs :+ RLEentry(0,1,aggedTuple)
      return
    }

    // aggregate each non-empty groups
    var aggedCount = 0
    for (key <- inputTuplesGrouped.keys) {
      var group = inputTuplesGrouped(key)
      var aggedTuple:Tuple = IndexedSeq()
      // add keys first
      aggedTuple = aggedTuple ++ key.asInstanceOf[Tuple]
      // for each aggregation, map group to agg args and reduce to get the final aggregated value
      // append all the aggregated values to get final aggregated tuple for a group
      for (agg <- aggCalls) {
        var mappedGroup = group.map(rle => agg.getArgument(rle.value, rle.length))
        var aggedVal = mappedGroup.reduce(agg.reduce)
        aggedTuple = aggedTuple :+ aggedVal
      }
      allAggedRLEs = allAggedRLEs :+ RLEentry(aggedCount, 1, aggedTuple)
      aggedCount += 1
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (nextTupleInd < allAggedRLEs.length) {
      var nextTuple = allAggedRLEs(nextTupleInd)
      nextTupleInd += 1
      return Option(nextTuple)
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
