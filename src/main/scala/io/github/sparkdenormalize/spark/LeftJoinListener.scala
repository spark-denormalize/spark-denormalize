package io.github.sparkdenormalize.spark

import java.io.PrintWriter

import scala.collection.mutable

import io.github.sparkdenormalize.spark.LeftJoinListener.{getRowCountLinageStrings, JoinData}
import io.github.sparkdenormalize.spark.SparkPlanGraph.NodeData
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener

// TODO:
//  - constructor should take a mapping from datasource -> attribute set
//  - what if two data source have identical attribute set?
//    (this can probably only happen if you have a `SELECT *` virtual data source)
//     - this is fine, because we know that leaf nodes can never be virtual data sources
//  - this needs unit tests for SparkPlans with all types of transformations:
//     - joins (non-left)
//     - filters
//     - group by
//     - windowing clause
//     - explode clause
//  - constructor should take thread-safe mutable variables, to allow propagation of data back to OutputDataset
class LeftJoinListener() extends QueryExecutionListener with Logging {

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logWarning(s"QUERY '${funcName}' -- FAILURE")
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logWarning(s"QUERY '${funcName}' -- SUCCESS")

    val sparkPlan: SparkPlan = qe.executedPlan
    val graph: SparkPlanGraph = SparkPlanGraph(sparkPlan)

    // TODO: remove after debug
    val dotString: String = graph.asDotString(maxLabelWidth = 100)
    new PrintWriter("spark_plan.dot") {
      write(dotString); close()
    }

    // TODO:
    //  - can we filter the join nodes we look at to only be LEFT joins,
    //    (as the user may use other joins in ComplexVirtualDataSource)
    //     - !! we also need to exclude left joins which are in ComplexVirtualDataSource
    //     - ?? how will this impact the linage printing when we detect a row count increase?
    graph.joinNodes.toSeq
      .sortWith(_ > _)
      .foreach { id: Long =>
        LeftJoinListener.getJoinData(id, graph) match {

          case Right(joinData: JoinData) => {
            val leftRowCount = joinData.leftSide.rowCount
            val rightRowCount = joinData.rightSide.rowCount
            val outputRowCount = joinData.output.rowCount

            if (leftRowCount != outputRowCount) {

              // get the linage path from the 'local root' of each join input
              //
              // we define the 'local root' as the upper-left-most ancestor of a node, this is because:
              //   0. all joins are left joins
              //   1. we check joins in descending order of their node-id (which is equivalent to picking
              //      the upper-right-most unchecked join)
              //   2. because of 0 and 1, we know that all upstream joins (on the right side) have not increased
              //      row count, so the issue must lie with the table which those upstream joins have been joined to
              //   3. therefore, the data source which caused the duplication is the left-most ancestor
              //      of the right join input
              //
              // we care about the linage (not just the data source itself) because it helps users
              // understand the chain of transformations which could have changed the number of rows in a source,
              // mainly: AGGREGATIONS, FILTERS, GENERATORS, and JOINS
              val leftLinage: Seq[NodeData] = graph
                .getLeftLineage(joinData.leftSide.nodeId)
                .reverse
              val rightLinage: Seq[NodeData] = graph
                .getLeftLineage(joinData.rightSide.nodeId)
                .reverse

              // TODO: when getting DataSource names, remember that leaves are always non-virtual data sources
              logError(
                s"""row count increased after LEFT join:
                 |------------
                 |Row Counts
                 |------------
                 |Left:   ${leftRowCount}
                 |Right:  ${rightRowCount}
                 |Output: ${outputRowCount}
                 |
                 |------------
                 |Left Lineage - *only stages which could change row count*
                 |------------
                 |DATASOURCE: ${ /*TODO: display data source name*/ }
                 |${getRowCountLinageStrings(leftLinage).mkString("\n↓\n")}
                 |
                 |------------
                 |Right Lineage - *only stages which could change row count*
                 |------------
                 |DATASOURCE: ${ /*TODO: display data source name*/ }
                 |${getRowCountLinageStrings(rightLinage).mkString("\n↓\n")}
                 |""".stripMargin
              )
            }
          }

          case Left(ex) => {
            // TODO: this listener runs in a separate thread,
            //       we need to bubble exceptions to the main thread somehow
            throw ex
          }
        }

      }
  }
}

object LeftJoinListener {

  // this is the name spark internally uses for counting output rows of Reads/Joins/Filters/Aggregates
  val ROW_COUNT_METRIC_NAME = "number of output rows"

  /** Get row-count data about a join
    *
    * @param id the join node
    * @param graph the graph
    * @return data about the join
    */
  private def getJoinData(
      id: Long,
      graph: SparkPlanGraph
  ): Either[IllegalArgumentException, JoinData] = {
    val parents: Set[Long] = graph.getNodeParents(id)
    if (parents.size != 2) {
      Left(
        new IllegalArgumentException(
          s"node #${id} is not a join node (joins have 2 parents, but it has: ${parents.size})"
        )
      )
    }

    // validate the metric exists on the output node
    val outputNode: NodeData = graph.getNodeData(id)
    if (!outputNode.metrics.contains(ROW_COUNT_METRIC_NAME)) {
      Left(
        new IllegalArgumentException(
          s"node #${id} is not a join node (it has no metric named: '${ROW_COUNT_METRIC_NAME}')"
        )
      )
    }

    // look for first ancestor of the left input with ROW_COUNT_METRIC_NAME
    val leftAncestor: NodeData = findAncestorWithRowCount(
      parents.min, // left side always has lower ID (due to SparkPlan structure)
      graph
    ) match {
      case Right(value) => graph.getNodeData(value)
      case Left(ex)     => return Left(ex) // scalastyle:ignore return
    }

    // look for first ancestor of the right input with ROW_COUNT_METRIC_NAME
    val rightAncestor: NodeData = findAncestorWithRowCount(
      parents.max, // right side always has higher ID (due to SparkPlan structure)
      graph
    ) match {
      case Right(value) => graph.getNodeData(value)
      case Left(ex)     => return Left(ex) // scalastyle:ignore return
    }

    Right(
      JoinData(
        leftSide = RowCountData(
          nodeId = leftAncestor.id,
          rowCount = leftAncestor.metrics(ROW_COUNT_METRIC_NAME).value,
          attributeSet = leftAncestor.outAttributeSet
        ),
        rightSide = RowCountData(
          nodeId = rightAncestor.id,
          rowCount = rightAncestor.metrics(ROW_COUNT_METRIC_NAME).value,
          attributeSet = rightAncestor.outAttributeSet
        ),
        output = RowCountData(
          nodeId = outputNode.id,
          rowCount = outputNode.metrics(ROW_COUNT_METRIC_NAME).value,
          attributeSet = outputNode.outAttributeSet
        )
      )
    )

  }

  /** Find the first ancestor of a node which has ROW_COUNT_METRIC_NAME (or the node itself) */
  private def findAncestorWithRowCount(
      id: Long,
      graph: SparkPlanGraph
  ): Either[IllegalArgumentException, Long] = {
    val ancestor: Option[Long] =
      try {
        graph.findAncestorWithMetric(
          startNode = id,
          metricName = ROW_COUNT_METRIC_NAME,
          includeStart = true
        )
      } catch {
        case ex: IllegalArgumentException => return Left(ex) // scalastyle:ignore return
      }

    ancestor match {
      case Some(value) => Right(value)
      case None =>
        Left(
          new IllegalArgumentException(
            s"node #${id} has no ancestor with metric named: '${ROW_COUNT_METRIC_NAME}'"
          )
        )
    }
  }

  /** Unpack information from a node's linage into strings
    *
    *  NOTE:
    *   - we exclude linage stages which have no ROW_COUNT_METRIC_NAME,
    *     as they cannot change the row count
    *
    * @param linage the sequence of node data representing the linage of a node
    * @return a sequence of strings with linage information
    */
  private def getRowCountLinageStrings(linage: Seq[NodeData]): Seq[String] = {
    linage
      .filter { node: NodeData =>
        // TODO:
        //  - should we exclude Joins, because we know they aren't the problem,
        //    (and could confuse the user)

        // ignore linage stages which dont change the number of rows
        node.metrics.contains(ROW_COUNT_METRIC_NAME)
      }
      .map { node: NodeData =>
        val lines = mutable.ArrayBuffer[String]()
        lines += s"STAGE: ${node.name}"
        if (node.groupingKeys.isDefined) {
          lines += s"EXPRESSION (GROUP-BY): [${node.groupingKeys.get.mkString(",")}]"
        }
        if (node.filter.isDefined) {
          lines += s"EXPRESSION (FILTER): ${node.filter.get}"
        }
        if (node.generator.isDefined) {
          lines += s"EXPRESSION (GENERATOR): ${node.generator.get}"
        }
        if (node.projections.isDefined) {
          lines += s"EXPRESSION (PROJECTIONS): ${node.projections.get}"
        }
        if (node.joinType.isDefined) {
          lines += s"JOIN-TYPE: ${node.joinType.get}"
        }
        lines += s"OUTPUT SCHEMA: ${node.outAttributeSet}" // TODO: remove or truncate this
        lines += s"OUTPUT ROWS: ${node.metrics(ROW_COUNT_METRIC_NAME).value}"
        lines.mkString("\n")
      }
  }

  case class RowCountData(nodeId: Long, rowCount: Long, attributeSet: AttributeSet)

  case class JoinData(leftSide: RowCountData, rightSide: RowCountData, output: RowCountData)

}
