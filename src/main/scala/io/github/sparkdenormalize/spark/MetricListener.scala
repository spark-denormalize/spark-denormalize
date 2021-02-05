package io.github.sparkdenormalize.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MetricListener extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logWarning(s"QUERY (SUCCESS) -- ${qe.sparkPlan.metrics} -- ${qe.sparkPlan}")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logWarning(s"QUERY (FAILURE) -- ${qe.sparkPlan}")
  }
}
