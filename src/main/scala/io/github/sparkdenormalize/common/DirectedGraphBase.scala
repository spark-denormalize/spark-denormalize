package io.github.sparkdenormalize.common

import java.io.StringWriter

import scala.collection.JavaConverters._

import io.github.sparkdenormalize.common.DirectedGraphBase.LongDefaultEdge
import org.jgrapht.graph.{DefaultEdge, SimpleDirectedGraph}
import org.jgrapht.nio.DefaultAttribute
import org.jgrapht.nio.dot.DOTExporter

trait DirectedGraphBase {

  protected val graph: SimpleDirectedGraph[Long, LongDefaultEdge]

  /** Get a label for each vertex in the graph, as a sequence of strings corresponding to lines */
  protected def getVertexLabel(vertexId: Long): Seq[String]

  /** Get a label for each edge in the graph, as a sequence of strings corresponding to lines */
  protected def getEdgeLabel(sourceVertexId: Long, targetVertexId: Long): Seq[String]

  /** Get the children of a vertex
    *
    * @param vertexId the vertex ID
    * @return the vertex IDs of the children
    * @throws IllegalArgumentException if the graph has no such vertex
    */
  protected def getVertexChildren(vertexId: Long): Set[Long] = {
    require(graph.containsVertex(vertexId), s"graph has no vertex with id: #${vertexId}")
    graph
      .outgoingEdgesOf(vertexId)
      .asScala
      .map(graph.getEdgeTarget)
      .toSet
  }

  /** Get the parents of a vertex
    *
    * @param vertexId the vertex ID
    * @return the vertex IDs of the parents
    * @throws IllegalArgumentException if the graph has no such vertex
    */
  protected def getVertexParents(vertexId: Long): Set[Long] = {
    // TODO: this should only return Option[Long] because its a DAG
    require(graph.containsVertex(vertexId), s"graph has no vertex with id: #${vertexId}")
    graph
      .incomingEdgesOf(vertexId)
      .asScala
      .map(graph.getEdgeSource)
      .toSet
  }

  /** Return a String containing this graph in Graphviz dot format
    *
    * @param maxLabelWidth the maximum character-width before node labels are truncated
    * @return a string in Graphviz dot format
    */
  def asDotString(maxLabelWidth: Int): String = {
    val exporter: DOTExporter[Long, LongDefaultEdge] = new DOTExporter((id: Long) => id.toString)

    // define VERTEX attribute provider
    exporter.setVertexAttributeProvider { id: Long =>
      val labelLines: Seq[String] = getVertexLabel(id)
      val safeLabelLines: Seq[String] = labelLines.map { line: String =>
        if (line.length > maxLabelWidth) {
          line.slice(0, maxLabelWidth) + "..."
        } else {
          line
        }
      }
      Map("label" -> DefaultAttribute.createAttribute(safeLabelLines.mkString("\n"))).asJava
    }

    // define EDGE attribute provider
    exporter.setEdgeAttributeProvider { edge: LongDefaultEdge =>
      val labelLines: Seq[String] = getEdgeLabel(edge.getSourceLong, edge.getTargetLong)
      val safeLabelLines: Seq[String] = labelLines.map { line: String =>
        if (line.length > maxLabelWidth) {
          line.slice(0, maxLabelWidth) + "..."
        } else {
          line
        }
      }
      Map("label" -> DefaultAttribute.createAttribute(safeLabelLines.mkString("\n"))).asJava
    }

    val writer = new StringWriter()
    exporter.exportGraph(graph, writer)
    writer.toString
  }

}

object DirectedGraphBase {

  /** A DefaultEdge which lets us extract a source/target as scala Long */
  class LongDefaultEdge extends DefaultEdge {

    def getSourceLong: Long = {
      this.getSource.asInstanceOf[Long]
    }

    def getTargetLong: Long = {
      this.getTarget.asInstanceOf[Long]
    }

  }

}
