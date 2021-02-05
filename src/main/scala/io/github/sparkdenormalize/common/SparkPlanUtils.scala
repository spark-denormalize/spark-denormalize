package io.github.sparkdenormalize.common

import scala.annotation.tailrec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

/** Utility methods related to [[org.apache.spark.sql.execution.SparkPlan]] */
object SparkPlanUtils extends Logging {

  /** XXXX
    *
    * @param plan
    * @param metricName
    * @return
    */
  @tailrec
  def firstChildWithMetric(
      plan: SparkPlan,
      metricName: String
  ): SparkPlan = {
    if (plan.metrics.contains(metricName)) {
      plan
    } else {
      val planChildren: Seq[SparkPlan] = plan.children
      val numChildren = planChildren.size

      if (numChildren == 1) {
        firstChildWithMetric(planChildren.head, metricName)
      } else if (numChildren == 0) {
        // TODO:
        throw new Exception(s"${plan}: has no more children")
      } else {
        // TODO:
        throw new Exception(s"${plan}: has more than one child")
      }
    }
  }
}
