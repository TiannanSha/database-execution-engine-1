package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Reconstruct[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  var outputs:IndexedSeq[RLEentry] = IndexedSeq()
  var emittedCount = 0
  var leftInputs:IndexedSeq[(Long, Tuple)] = IndexedSeq()
  var rightInputs:IndexedSeq[(Long, Tuple)] = IndexedSeq()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init state vars
    outputs = IndexedSeq()
    emittedCount = 0
    leftInputs = IndexedSeq()
    rightInputs = IndexedSeq()

    val leftIter = left.iterator
    val rightIter = right.iterator
    // read all left RLEentries with repeated entries all explicitly stored
    // [(vid, tuple)], e.g. [(0,t1), (1,t1), (8,t2), (9,t2), (10,t2)]
    while (leftIter.hasNext){
      val l = leftIter.next()
      for (i <- 0 until l.length.toInt){
        val vid = l.startVID
        leftInputs = leftInputs :+ (vid+i, l.value)
      }
    }

    while (rightIter.hasNext){
      val r = rightIter.next()
      for (i <- 0 until r.length.toInt){
        val vid = r.startVID
        rightInputs = rightInputs :+ (vid+i, r.value)
      }
    }
    // if either side is empty, there's nothing to do
    if (leftInputs.isEmpty || rightInputs.isEmpty) return

    println(s"leftInputs = ${leftInputs.length}")
    println(s"rightInputs = ${rightInputs.length}")

    // loop thru leftInputs and rightInputs in a merge sort fashion
    var i=0 // index for leftInputs
    var j=0 // index for rightInputs
    var outInd=0 // index for output
    while (i<leftInputs.length && j<rightInputs.length){
      var l = leftInputs(i)
      var r = rightInputs(j)
      // ._1 is the vid
      val lVid = l._1
      val lTuple = l._2
      val rVid = r._1
      val rTuple = r._2
      if (lVid < rVid) {
        // left vid is too small, move left index to find bigger left vid
        i+=1
      } else if (rVid < lVid){
        j+=1
      } else {
        // lVid==rVid match, "insert" (lVid, reconstructed) to outputs
        val reconstructed = lTuple ++ rTuple
        // since within one side, vid are strictly decreasing, when lVid = rVid
        // we should move on to compare larger vids
        i+=1
        j+=1

        // if reconstructed tuple is same as the last one and the vid is continuous
        // oldRLE = (startVid=0,len=2,t), new=(vid=2,t)
        // update the last RLEentry in the outputs
        if (outInd>0 && reconstructed==outputs(outInd-1).value
          && outputs(outInd-1).startVID + outputs(outInd-1).length == lVid) {
          outputs = outputs.dropRight(1)
          outputs = outputs :+ RLEentry(outputs(outInd-1).startVID,
            outputs(outInd-1).length+1, outputs(outInd-1).value)
        } else {
          // just need to add a new RLEentry
          outputs = outputs :+ RLEentry(lVid, 1, reconstructed)
        }

//        if (outInd==0 ||
//          (  outInd>0 && !reconstructed==oldLastOutput.value
//            && (oldLastOutput.startVID + )  // == or eq or equal?
//        ) {
//          outputs = outputs :+ RLEentry(lVid+i, 1, reconstructed)
//          outInd += 1
//        } else {
//          // same as the last reconstructed tuple
//          // change the last element to be the old last RlE with length+1
//          assert(reconstructed==outputs(outInd-1).value)
//
//        }
      }

    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (emittedCount < outputs.length) {
      val output = outputs(emittedCount)
      emittedCount += 1
      Some(output)
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
