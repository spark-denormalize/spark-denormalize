package io.github.sparkdenormalize.spark

import java.io.StringWriter
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.github.sparkdenormalize.spark.SparkPlanGraph.NodeData
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultEdge, SimpleDirectedGraph}
import org.jgrapht.nio.DefaultAttribute
import org.jgrapht.nio.dot.DOTExporter

// TODO: move to DirectedGraphBase in utils
class SparkPlanGraph private (
    graph: SimpleDirectedGraph[Long, DefaultEdge],
    vertexMap: Map[Long, NodeData]
) {

  /** The set of node IDs corresponding to Join nodes */
  val joinNodes: Set[Long] =
    vertexMap.keySet.filter { id: Long =>
      // nodes with in-degree = 2 must be join nodes
      graph.inDegreeOf(id) == 2
    }

  /** Get the NodeData associate with a given node
    *
    * @param id the node id
    * @return the NodeData
    * @throws IllegalArgumentException if the graph has no such node
    */
  def getNodeData(id: Long): NodeData =
    vertexMap.get(id) match {
      case Some(value) => value
      case None        => throw new IllegalArgumentException(s"graph has no node #${id}")
    }

  /** Get the sequence of nodes found by traversing up and to the left of a starting node
    *
    * @param startNode the id of node to start traversing from
    * @return the sequence of nodes found
    * @throws IllegalArgumentException if the graph has no such startNode
    */
  def getLeftLineage(startNode: Long): Seq[NodeData] = {
    require(vertexMap.contains(startNode), s"graph has no node #${startNode}")

    var node: NodeData = vertexMap(startNode)
    var nodeParents: Set[Long] = getNodeParents(startNode)
    val nodeSequence = mutable.ArrayBuffer[NodeData](node)

    // loop until we hit a leaf node
    while (nodeParents.nonEmpty) {
      node = vertexMap(nodeParents.min)
      nodeParents = getNodeParents(node.id)
      nodeSequence += node
    }

    nodeSequence
  }

  /** Get the parents of a given node
    *
    * @param id the node id
    * @return the IDs of the parents
    * @throws IllegalArgumentException if the graph has no such node
    */
  def getNodeParents(id: Long): Set[Long] = {
    require(vertexMap.contains(id), s"graph has no node #${id}")
    graph
      .incomingEdgesOf(id)
      .asScala
      .map(graph.getEdgeSource)
      .toSet
  }

  /** Find the first ancestor of a node which has a specific metric
    *
    * If it encounters a branch, will throw: [[IllegalArgumentException]]
    *
    * @param startNode the id of node to start searching from
    * @param metricName the name of the metric to search for
    * @param includeStart if we should consider the start node
    * @return the ID of the found node, if any
    * @throws IllegalArgumentException if a branch is encountered
    * @throws IllegalArgumentException if the graph has no such startNode
    */
  def findAncestorWithMetric(
      startNode: Long,
      metricName: String,
      includeStart: Boolean
  ): Option[Long] = {
    require(vertexMap.contains(startNode), s"graph has no node #${startNode}")

    var node: NodeData = vertexMap(startNode)
    var nodeHasMetric: Boolean = if (includeStart) node.metrics.contains(metricName) else false
    var nodeParents: Set[Long] = getNodeParents(startNode)

    // loop until we hit a leaf node, or the current node has the required metric
    while (!nodeHasMetric && nodeParents.nonEmpty) {
      require(nodeParents.size == 1, s"encountered branch in tree at node #${node.id}")
      node = vertexMap(nodeParents.head)
      nodeHasMetric = node.metrics.contains(metricName)
      nodeParents = getNodeParents(node.id)
    }

    if (nodeHasMetric) Some(node.id) else None
  }

  /** Return a String containing this graph in Graphviz dot format
    *
    * @param maxLabelWidth the maximum character-width before node labels are truncated
    * @return a string in Graphviz dot format
    */
  def asDotString(maxLabelWidth: Int): String = {
    val exporter: DOTExporter[Long, DefaultEdge] = new DOTExporter((id: Long) => id.toString)
    exporter.setVertexAttributeProvider { id: Long =>
      val labelLines: Seq[String] = Seq(
        id.toString,
        vertexMap(id).name,
        vertexMap(id).outAttributeSet.mkString(","),
        vertexMap(id).metrics.keys.mkString(","),
        vertexMap(id).groupingKeys.map(_.mkString(",")).getOrElse("..."),
        vertexMap(id).filter.map(_.toString).getOrElse("...")
      )
      val safeLabelLines: Seq[String] = labelLines.map { line: String =>
        if (line.length > maxLabelWidth) {
          line.slice(0, maxLabelWidth) + "..."
        } else {
          line
        }
      }
      Map(
        "label" -> DefaultAttribute.createAttribute(safeLabelLines.mkString("\n"))
      ).asJava
    }

    val writer = new StringWriter()
    exporter.exportGraph(graph, writer)
    writer.toString
  }

}

object SparkPlanGraph {

  /** Build a SparkPlanGraph from a SparkPlan */
  def apply(plan: SparkPlan): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val graph = new SimpleDirectedGraph[Long, DefaultEdge](classOf[DefaultEdge])
    val vertexMap = mutable.HashMap[Long, NodeData]()

    // add root node
    val rootNode = buildNodeData(nodeIdGenerator.getAndIncrement(), plan)
    graph.addVertex(rootNode.id)
    vertexMap += rootNode.id -> rootNode

    // recursively add child nodes
    updateGraph(plan, nodeIdGenerator, graph, vertexMap, rootNode)

    // return
    new SparkPlanGraph(graph, vertexMap.toMap)
  }

  /** Build a NodeData object from a SparkPlan object */
  private def buildNodeData(nodeId: Long, sparkPlan: SparkPlan): NodeData = {

    val nodeName: String = sparkPlan.nodeName
    val nodeMetrics: Map[String, SQLMetric] = sparkPlan.metrics.map {
      case (key: String, metric: SQLMetric) =>
        (metric.name.getOrElse(key), metric)
    }
    val nodeOutAttributeSet: AttributeSet = sparkPlan.outputSet

    sparkPlan match {
      case p: BaseAggregateExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          groupingKeys = Some(p.groupingExpressions)
        )
      case p: FilterExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          filter = Some(p.condition)
        )
      case p: GenerateExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          generator = Some(p.generator)
        )
      case p: ExpandExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          projections = Some(p.projections)
        )

      // we must list out each join type explicitly, because they do not have a common trait
      case p: BroadcastNestedLoopJoinExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          joinType = Some(p.joinType)
        )
      case p: HashJoin =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          joinType = Some(p.joinType)
        )
      case p: SortMergeJoinExec =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet,
          joinType = Some(p.joinType)
        )

      case _ =>
        NodeData(
          id = nodeId,
          name = nodeName,
          metrics = nodeMetrics,
          outAttributeSet = nodeOutAttributeSet
        )

    }

  }

  /** Mutate the provided Graph with data from SparkPlan */
  private def updateGraph(
      plan: SparkPlan,
      nodeIdGenerator: AtomicLong,
      graph: Graph[Long, DefaultEdge],
      vertexMap: mutable.Map[Long, NodeData],
      parent: NodeData
  ): Unit = {
    // NOTE: this is based on `SparkPlanInfo.fromSparkPlan()`, and needs to be updated whenever that is
    val children: Seq[SparkPlan] = plan match {
      case ReusedExchangeExec(_, child)    => child :: Nil
      case ReusedSubqueryExec(child)       => child :: Nil
      case a: AdaptiveSparkPlanExec        => a.executedPlan :: Nil
      case stage: QueryStageExec           => stage.plan :: Nil
      case inMemTab: InMemoryTableScanExec => inMemTab.relation.cachedPlan :: Nil
      case _                               => plan.children ++ plan.subqueries
    }

    children foreach { childPlan: SparkPlan =>
      val childNode = buildNodeData(nodeIdGenerator.getAndIncrement(), childPlan)

      // add child to graph
      graph.addVertex(childNode.id)
      vertexMap += childNode.id -> childNode
      graph.addEdge(childNode.id, parent.id)

      // recurse into the child
      updateGraph(childPlan, nodeIdGenerator, graph, vertexMap, childNode)
    }

  }

  /** A class which stores the data of nodes in a SparkPlanGraph */
  case class NodeData(
      id: Long,
      name: String,
      metrics: Map[String, SQLMetric],
      outAttributeSet: AttributeSet,
      groupingKeys: Option[Seq[NamedExpression]] = None,
      filter: Option[Expression] = None,
      generator: Option[Generator] = None,
      projections: Option[Seq[Seq[Expression]]] = None,
      joinType: Option[JoinType] = None
  )

}
