package ch.epfl.dias.cs422.rel.early.volcano.rle.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalDecode
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter

import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * RelRule (optimization rule) that finds a filter above a decode
  * and pushes it bellow it.
  *
  * To use this rule: FilterDecodeTransposeRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class FilterDecodeTransposeRule protected (config: RelRule.Config)
    extends RelRule(config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: LogicalFilter = call.rel(0)
    val decode: LogicalDecode = call.rel(1)
    // my code
    val equiv = Map(decode->filter).asJava
    val decodeRel = decode.getInput
    val filterRel = filter.getInput

//    println("**** in onMatch *****")
//    println(s"**call.rel=")
//    println(s"**filter = $filter")
//    println(s"**filter.getInput = ${filter.getInput}")
//    println(s"**decode = $decode")
//    println(s"**decode.getInput = ${decode.getInput}")

    call.transformTo(
      // mycode
      decode.copy(filter.copy(filter.getTraitSet, decode.getInput, filter.getCondition))
      //decode
    )
  }
}

object FilterDecodeTransposeRule {

  /**
    * Configuration for a [[FilterDecodeTransposeRule]]
    */
  val INSTANCE = new FilterDecodeTransposeRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
    // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalFilter]
        b.operand(classOf[LogicalFilter])
          // that has inputs:
          .oneInput(b1 =>
            // A node that is a LogicalDecode
            b1.operand(classOf[LogicalDecode])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
