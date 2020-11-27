package io.github.sparkdenormalize.core.output

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.github.sparkdenormalize.common.DirectedGraphBase
import io.github.sparkdenormalize.common.DirectedGraphBase.LongDefaultEdge
import io.github.sparkdenormalize.common.SparkColumnMethods.{
  resolveAttributes,
  UnresolvableAttributeException
}
import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.{
  GroupByFieldMissingMode,
  JoinFieldMissingMode,
  PathMissingMode,
  SnapshotMissingMode,
  SnapshotPreference
}
import io.github.sparkdenormalize.config.resource.spec.TimeOffset
import io.github.sparkdenormalize.core.SnapshotTime
import io.github.sparkdenormalize.core.datasource.DataSource
import io.github.sparkdenormalize.core.output.DataModelGraph.{
  dataFirstThenLexicographic,
  EdgeData,
  OtherDataSourceConfig,
  RootDataSourceConfig,
  SkipSnapshotException,
  VertexData
}
import io.github.sparkdenormalize.core.relationship.DataRelationship
import io.github.sparkdenormalize.core.relationship.DataRelationship.JoinKeyComponent
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{collect_list, expr, struct}
import org.jgrapht.graph.SimpleDirectedGraph

/** A graph representation of a data model, with methods to read data at specific snapshot times */
class DataModelGraph private (
    protected val graph: SimpleDirectedGraph[Long, LongDefaultEdge],
    rootConfig: RootDataSourceConfig,
    vertexMap: Map[Long, VertexData],
    edgeMap: Map[(Long, Long), EdgeData]
)(implicit ss: SparkSession)
    extends Logging
    with DirectedGraphBase {

  /** Get a label for each vertex in the graph, as a sequence of strings corresponding to lines */
  override protected def getVertexLabel(vertexId: Long): Seq[String] = {
    val vertexData: VertexData = vertexMap(vertexId)
    Seq(
      s"${vertexData.dataSource.refString}",
      s"groupBy: ${vertexData.config.map(_.groupByFieldNames).getOrElse("N/A")}"
    )
  }

  /** Get a label for each edge in the graph, as a sequence of strings corresponding to lines */
  override protected def getEdgeLabel(sourceVertexId: Long, targetVertexId: Long): Seq[String] = {
    val edgeData: EdgeData = edgeMap((sourceVertexId, targetVertexId))
    Seq(
      s"${edgeData.relationship.refString}",
      s"antiPriority: ${edgeData.config.antiPriority}"
    )
  }

  /** Return available snapshot times (based off the root DataSource only)
    *
    * @param excludeSingle dont return these snapshot
    * @param excludeRanges dont return snapshots in these ranges (inclusive)
    * @param excludeAfter  dont return snapshots after this time (inclusive)
    * @param excludeBefore dont return snapshots before this time (inclusive)
    * @return the set of available snapshot times
    */
  def availableRootSnapshotTimes(
      excludeAfter: SnapshotTime,
      excludeBefore: SnapshotTime,
      excludeSingle: Set[SnapshotTime],
      excludeRanges: Seq[(SnapshotTime, SnapshotTime)]
  ): Set[SnapshotTime] = {
    rootConfig.snapshotFilter match {
      case RootSnapshotFilter.FromInterval(preference, intervalStartDate, intervalLength) =>
        // TODO: `excludeBefore` and `excludeAfter` will need new configs in
        //       `spec.dataModel.rootDataSource.snapshotFilter`
        //  - excludeBefore: is the time before the present after which we will not check for snapshots
        //  - excludeAfter: is the time after the present after which we will not check for snapshots
        //     - ?? default in config to the present time

        // TODO: ?? should there be methods on RootSnapshotFilter

        // NOTE: this is only based on the root
        //  - runtime specific errors (like missing snapshots/schema) are handled as we read/write each snapshot
        //     - meaning a job might partially complete the backlog before failing when it gets to a snapshot
        //       that it cant handle
        ???
    }
  }

  /** Read an OutputSnapshot for a root SnapshotTime
    *
    * @param rootSnapshotTime the root snapshot time
    * @return the OutputSnapshot containing the requested data
    */
  def readSnapshot(rootSnapshotTime: SnapshotTime): OutputSnapshot = {
    val (data: Option[DataFrame], _) =
      readDataFrame(parentId = 0, rootSnapshotTime = rootSnapshotTime)
    OutputSnapshot(data = data.get, snapshotTime = rootSnapshotTime)
  }

  /** Internal method which recursively reads data from this data model
    *
    * @param parentId the vertex Id to start the recursive read at
    * @param rootSnapshotTime the snapshot time we want to read the root vertex at
    * @return a tuple of (data, groupByColumns)
    *         - data: a DataFrame including all data from `parentId` and the vertices nested
    *                 under it in the data model, or None if this data should be skipped for some reason
    *         - groupByColumns: a sequence of resolved columns which `data` should be grouped by
    */
  private def readDataFrame(
      parentId: Long,
      rootSnapshotTime: SnapshotTime
  ): (Option[DataFrame], Seq[Column]) = {
    require(vertexMap.contains(parentId), s"graph has no vertex with id: #${parentId}")

    val parent: VertexData = vertexMap(parentId)
    val parentIsRoot: Boolean = parent.config.isEmpty

    // calculate the SnapshotTime to read data from parent
    val parentReadTime: SnapshotTime =
      if (parentIsRoot) {
        rootSnapshotTime
      } else {
        // get the config for this node
        //  - note, `.get` is safe because all non-root nodes have config defined
        val parentConfig: OtherDataSourceConfig = parent.config.get

        val minSnapshotTime = rootSnapshotTime.minus(parentConfig.snapshotTimeBefore)
        val maxSnapshotTime = rootSnapshotTime.plus(parentConfig.snapshotTimeAfter)

        // ask the data source if it has a snapshot available which meets our requirements
        val bestSnapshotTime: Option[SnapshotTime] = parent.dataSource.bestAvailableSnapshot(
          snapshotPreference = parentConfig.snapshotPreference,
          minSnapshotTime = minSnapshotTime,
          maxSnapshotTime = maxSnapshotTime
        )

        // handle the case when there is no snapshot available from the data source
        if (bestSnapshotTime.isEmpty) {
          val message = s"${parent.dataSource.refString} has no snapshot for: " +
            s"minSnapshotTime=${minSnapshotTime}, maxSnapshotTime=${maxSnapshotTime}"

          parentConfig.snapshotMissingMode match {
            case SnapshotMissingMode.ERROR =>
              throw new Exception(message) // TODO: custom exception object

            case SnapshotMissingMode.CONTINUE_IGNORE =>
              // returning None will cause us to not include data from this parent
              return (None, Seq()) // scalastyle:ignore return

            case SnapshotMissingMode.CONTINUE_WARN =>
              logWarning(message)
              // returning None will cause us to not include data from this parent
              return (None, Seq()) // scalastyle:ignore return

            case SnapshotMissingMode.SKIP_IGNORE =>
              throw new SkipSnapshotException(message)

            case SnapshotMissingMode.SKIP_WARN =>
              logWarning(message)
              throw new SkipSnapshotException(message)
          }
        }
        bestSnapshotTime.get
      }

    // read DataFrame of parent
    //  - note, we collect all raw columns into a struct called "data"
    val parentData: DataFrame = parent.dataSource
      .readAt(parentReadTime)
      .select(
        struct("*").as("data")
      )

    // get the group-by columns of the parent
    val groupByColumns: Seq[Column] =
      if (parentIsRoot) {
        // CASE 0: the root is not allowed to have any group-by columns
        Seq()
      } else {
        // CASE 1: non-root vertices might have group-by columns configured
        val parentConfig: OtherDataSourceConfig = parent.config.get
        parentConfig.groupByFieldNames.map { fieldName: String =>
          try {
            resolveAttributes(
              column = expr(fieldName),
              dataFrame = parentData,
              attributePrefix = Seq("data")
            )
          } catch {
            case ex: UnresolvableAttributeException =>
              // the message for any exceptions we throw
              val messageEx =
                s"failed to resolve group-by field against ${parent.dataSource.refString}"

              // the message for any logs we print
              val messageLog = messageEx + " (ERROR MESSAGE: " + ex.getMessage + ")"

              parentConfig.groupByFieldMissingMode match {
                case GroupByFieldMissingMode.ERROR =>
                  throw new Exception(messageEx, ex) // TODO: custom exception object

                case GroupByFieldMissingMode.CONTINUE_IGNORE =>
                  // returning None will cause us to not include data from this parent
                  return (None, Seq()) // scalastyle:ignore return

                case GroupByFieldMissingMode.CONTINUE_WARN =>
                  logWarning(messageLog)
                  // returning None will cause us to not include data from this parent
                  return (None, Seq()) // scalastyle:ignore return

                case GroupByFieldMissingMode.SKIP_IGNORE =>
                  throw new SkipSnapshotException(messageEx, ex)

                case GroupByFieldMissingMode.SKIP_WARN =>
                  logWarning(messageLog)
                  throw new SkipSnapshotException(messageEx, ex)
              }
          }
        }
      }

    // join parent to each of its children
    val joinedDataRaw: DataFrame = getVertexChildren(parentId).foldLeft(parentData) {

      (df: DataFrame, childId: Long) =>
        // recursively read data from this child
        val (childDataRawOpt: Option[DataFrame], childGroupByColumns: Seq[Column]) =
          readDataFrame(childId, rootSnapshotTime)

        // if the child's DataFrame is None, we skip this `foldLeft()`
        if (childDataRawOpt.isEmpty) {
          // return DataFrame without joining, continue to next `foldLeft()`
          df
        } else {
          val childDataRaw = childDataRawOpt.get

          // get the VertexData and EdgeData for this child
          val child: VertexData = vertexMap(childId)
          val edge: EdgeData = edgeMap((parentId, childId))

          // get the config for this child
          //  - note, `.get` is safe because a child cannot be a root, so will have config defined
          val childConfig: OtherDataSourceConfig = child.config.get

          // get the fieldName for this child
          val childFieldName: String = childConfig.fieldName

          // transform data by nesting into structs
          val childData: DataFrame =
            if (childGroupByColumns.isEmpty) {
              // CASE 0: construct an outer struct from the columns of this child
              childDataRaw
                .select(
                  struct("*").as(childFieldName)
                )
            } else {
              // CASE 1: construct an outer List[Struct] from the columns of this child
              childDataRaw
                .groupBy(childGroupByColumns: _*)
                .agg(
                  collect_list(struct("*")).as(childFieldName)
                )
            }

          // calculate join-key -- parent/left side
          //  - note, we use Option[] so that we can skip this fold with CONTINUE_IGNORE/CONTINUE_WARN
          val parentJoinColumnsOpt: Option[Seq[Column]] =
            try {
              Some(
                edge.relationship
                  .getJoinKey(parent.dataSource)
                  .map { joinKeyComponent: JoinKeyComponent =>
                    // this prefix will be applied to all unresolved fields
                    //  - note, this is required because we nest the raw columns into structs
                    val attributePrefix = Seq("data")

                    // resolve the attributes used by the query
                    //  - note, this tells spark explicitly what columns (from what DataFrame) are referenced, so
                    //    there is no reference ambiguity in situations like when multiple "customer_id" fields exist
                    val resolvedQuery: Column =
                      resolveAttributes(
                        column = joinKeyComponent.query,
                        dataFrame = parentData,
                        attributePrefix = attributePrefix,
                        expectedDataTypes = joinKeyComponent.fieldsMap
                      )

                    // cast the result of the query to the configured type
                    resolvedQuery.cast(joinKeyComponent.castDataType)
                  }
              )
            } catch {
              case ex: UnresolvableAttributeException =>
                // the message for any exceptions we throw
                val messageEx =
                  s"failed to resolve join-key field against ${parent.dataSource.refString}"

                // the message for any logs we print
                val messageLog = messageEx + " (ERROR MESSAGE: " + ex.getMessage + ")"

                // note, this uses the CHILD CONFIG, because we would exclude the child in the missing case
                childConfig.joinFieldMissingMode match {
                  case JoinFieldMissingMode.ERROR =>
                    throw new Exception(messageEx, ex) // TODO: custom exception object

                  case JoinFieldMissingMode.CONTINUE_IGNORE =>
                    // returning None will cause us to skip this `foldLeft()`
                    None

                  case JoinFieldMissingMode.CONTINUE_WARN =>
                    logWarning(messageLog)
                    // returning None will cause us to skip this `foldLeft()`
                    None

                  case JoinFieldMissingMode.SKIP_IGNORE =>
                    throw new SkipSnapshotException(messageEx, ex)

                  case JoinFieldMissingMode.SKIP_WARN =>
                    logWarning(messageLog)
                    throw new SkipSnapshotException(messageEx, ex)
                }
            }

          // calculate join-key -- child/right side
          //  - note, we use Option[] so that we can skip this fold with CONTINUE_IGNORE/CONTINUE_WARN
          val childJoinColumnsOpt: Option[Seq[Column]] =
            try {
              Some(
                edge.relationship
                  .getJoinKey(child.dataSource)
                  .map { joinKeyComponent: JoinKeyComponent =>
                    // this prefix will be applied to all unresolved fields
                    //  - note, this is required because we nest the raw columns into structs
                    val attributePrefix =
                      if (childGroupByColumns.nonEmpty) {
                        // CASE 0: group-by will cause join-keys to be at the root of the DataFrame structure
                        //  - note, non group-by key columns will be inside a list so cannot be used in the join-key,
                        //    therefore, we ensure join-keys only use the group-by keys when constructing the graph
                        Seq()
                      } else {
                        // CASE 1: nesting of raw columns will cause join-keys to be under the `{fieldName}.data` struct
                        Seq(childFieldName, "data")
                      }

                    // resolve the attributes used by the query
                    //  - note, this tells spark explicitly what columns (from what DataFrame) are referenced, so
                    //    there is no reference ambiguity in situations like when multiple "customer_id" fields exist
                    val resolvedQuery: Column =
                      resolveAttributes(
                        column = joinKeyComponent.query,
                        dataFrame = childData,
                        attributePrefix = attributePrefix,
                        expectedDataTypes = joinKeyComponent.fieldsMap
                      )

                    // cast the result of the query to the configured type
                    resolvedQuery.cast(joinKeyComponent.castDataType)
                  }
              )
            } catch {
              case ex: UnresolvableAttributeException =>
                // the message for any exceptions we throw
                val messageEx =
                  s"failed to resolve join-key field against ${child.dataSource.refString}"

                // the message for any logs we print
                val messageLog = messageEx + " (ERROR MESSAGE: " + ex.getMessage + ")"

                childConfig.joinFieldMissingMode match {
                  case JoinFieldMissingMode.ERROR =>
                    throw new Exception(messageEx, ex) // TODO: custom exception object

                  case JoinFieldMissingMode.CONTINUE_IGNORE =>
                    None // returning None will cause us to skip this `foldLeft()`

                  case JoinFieldMissingMode.CONTINUE_WARN =>
                    logWarning(messageLog)
                    None // returning None will cause us to skip this `foldLeft()`

                  case JoinFieldMissingMode.SKIP_IGNORE =>
                    throw new SkipSnapshotException(messageEx, ex)

                  case JoinFieldMissingMode.SKIP_WARN =>
                    logWarning(messageLog)
                    throw new SkipSnapshotException(messageEx, ex)
                }
            }

          // if either join column list is None, we skip this `foldLeft()`
          if (parentJoinColumnsOpt.isEmpty || childJoinColumnsOpt.isEmpty) {
            // return DataFrame without joining, continue to next `foldLeft()`
            df
          } else {
            // unpack the join-key column list options
            val parentJoinColumns: Seq[Column] = parentJoinColumnsOpt.get
            val childJoinColumns: Seq[Column] = childJoinColumnsOpt.get

            // construct the join expression
            val joinExpr: Column = {
              // should never trigger in real world, but enforces safety when zipping together
              assert(
                parentJoinColumns.size == childJoinColumns.size,
                "`parentJoinColumns` and `childJoinColumns` must have same number of elements, but got: " +
                  s"parentJoinColumns=${parentJoinColumns.size}, childJoinColumns=${childJoinColumns.size}"
              )
              // zip join-key components and reduce them so we support both single & compound type join-keys
              parentJoinColumns
                .zip(childJoinColumns)
                .map { case (parentKeyComponent: Column, childKeyComponent: Column) =>
                  parentKeyComponent === childKeyComponent
                }
                .reduce(_ && _)
            }

            // join this child to the DataFrame
            val dfTemp: DataFrame = df.join(
              right = childData,
              joinExprs = joinExpr,
              joinType = "left"
            )

            // get the list of columns which should be included in the output
            //  - note, this prevents group-by key(s) ending up in the output
            val oldColumns: Seq[Column] = df.columns.map(df.col)
            val newColumns: Seq[Column] = Seq(childData(childFieldName))

            // return Dataframe, continue to next `foldLeft()`
            dfTemp.select(oldColumns ++ newColumns: _*)
          }
        }
    }

    // sort the columns in dataFirstThenLexicographic order
    val sortedColumns: Seq[Column] = joinedDataRaw.columns
      .sortWith(dataFirstThenLexicographic)
      .map(joinedDataRaw.col)

    // re-order the columns
    val dfJoined: DataFrame = joinedDataRaw.select(sortedColumns: _*)

    (Some(dfJoined), groupByColumns)

    // TODO: attach the LeftJoinListener
  }

}

object DataModelGraph extends Logging {

  /** Build a DataModelGraph */
  def apply(
      rootDataSource: DataSource,
      rootDataSourceConfig: RootDataSourceConfig,
      otherDataSources: Seq[(DataSource, OtherDataSourceConfig)],
      dataRelationships: Seq[(DataRelationship, DataRelationshipConfig)],
      maxDepth: Long
  )(implicit ss: SparkSession): DataModelGraph = {

    val graph = new SimpleDirectedGraph[Long, LongDefaultEdge](classOf[LongDefaultEdge])
    val vertexMap = mutable.HashMap[Long, VertexData]()
    val inverseVertexMap = mutable.HashMap[DataSource, Long]()
    val edgeMap = mutable.HashMap[(Long, Long), EdgeData]()

    // ----------------
    // Vertices
    // ----------------
    var vertexId: Long = 0

    // add root DataSource to the graph
    graph.addVertex(vertexId)
    vertexMap += vertexId -> VertexData(rootDataSource)
    inverseVertexMap += rootDataSource -> vertexId

    // add all non-root DataSource to the graph
    otherDataSources foreach { case (source: DataSource, config: OtherDataSourceConfig) =>
      if (inverseVertexMap.contains(source)) {
        throw new Exception( // TODO: custom exception object
          s"${source.refString} exists at multiple positions in the graph"
        )
      }

      vertexId += 1
      graph.addVertex(vertexId)
      vertexMap += vertexId -> VertexData(source, Some(config))
      inverseVertexMap += source -> vertexId
    }

    // ----------------
    // Edges
    // ----------------
    // group the data relationships by antiPriority
    val dataRelationshipsByPriority: Map[Long, Seq[(DataRelationship, DataRelationshipConfig)]] =
      dataRelationships.groupBy(_._2.antiPriority)

    // get the list of antiPriority in ascending order
    val antiPrioritySeq: Seq[Long] =
      dataRelationshipsByPriority.keys.toSeq.sortWith(_ < _)

    var currentDepth = 0L
    val unvisitedVertices = mutable.HashSet[Long](graph.vertexSet().asScala.toSeq: _*)
    val activeVertices = mutable.HashSet[Long](0)

    // TODO: remove after DEBUG
    logWarning("--- DataModelGraph ---")

    // at each iteration of this loop, we consider vertices at a `currentDepth` from the root DataSource,
    // and attempt to find unvisited DataSources at `currentDepth + 1` which we can induct into the DataModel
    //  - this iterative construction, ensures each DataSource is included with the lowest `number of hops from root`,
    //  - note, this is slightly different from the typical minimum-spanning-tree, as we only consider the
    //    `antiPriority` weighting in cases where two paths have the same `number of hops from root`
    //  - in cases where there are two paths with the same `number of hops from root` and `antiPriority`,
    //    there would be two valid join paths over the DataModel, so we must error and tell the user what went wrong
    while (activeVertices.nonEmpty) {
      val nextActiveVertices = mutable.HashSet[Long]()

      // TODO: remove after DEBUG
      logWarning("↓")
      logWarning(s"currentDepth: ${currentDepth}")
      logWarning(s"unvisitedVertices: ${unvisitedVertices}")
      logWarning(s"activeVertices: ${activeVertices}")

      // mark our current `activeVertices` as visited (so we dont try and traverse to them this iteration)
      unvisitedVertices --= activeVertices

      // get the DataSources which correspond to the unvisited vertex IDs
      val unvisitedDataSources: mutable.HashSet[DataSource] =
        unvisitedVertices.map(vertexMap(_).dataSource)

      // loop over each `antiPriority` in ascending order:
      //  - note, there can be multiple distinct DataRelationships for each `antiPriority`
      //  - if there are two edges targeting the same unvisited DataSource, at the same `currentDepth`,
      //    we want to take the one with lower `antiPriority`
      //  - by looping in ascending `antiPriority` order, we get the edge with lowest `antiPriority` by construction,
      //    this is because once we find an edge targeting a DataSource, we stop considering that DataSource
      //    for DataRelationships with higher `antiPriority`
      //  - in cases where multiple DataRelationships of the same `antiPriority` target the same DataSource,
      //    we must throw an error
      antiPrioritySeq foreach { antiPriority: Long =>
        // create a map with {"vertexId" -> "sequence of path-triples targeting vertexId"}
        //  - for detecting when multiple DataRelationships at the same `antiPriority` target the same vertex
        //  - note, this will be checked as we move on to the next `antiPriority`,
        //    and if we have visited the same vertex more than once, an error will be thrown
        val visitedVertexCounter = mutable.HashMap[Long, mutable.ArrayBuffer[PathTriple]]()

        // get the DataRelationships at this `antiPriority`
        val relationships: Seq[(DataRelationship, DataRelationshipConfig)] =
          dataRelationshipsByPriority(antiPriority)

        // process each relationship at this `antiPriority` level
        relationships foreach {
          case (relationship: DataRelationship, relationshipConfig: DataRelationshipConfig) => {

            // process each active-vertex/source-vertex for this relationship
            activeVertices foreach { sourceId: Long =>
              val sourceVertex: VertexData = vertexMap(sourceId)

              // only continue with the source-vertex, if this relationship contains it
              if (relationship.hasDataSource(sourceVertex.dataSource)) {

                // get unvisited DataSources which are in this relationship
                val targetDataSources: mutable.HashSet[DataSource] =
                  relationship.intersectMutable(unvisitedDataSources)

                // filter the excludeSets of this relationship, to ones which contain this source-vertex
                val filteredExcludeSets: Seq[Set[DataSource]] = relationshipConfig.excludeSets
                  .filter(_.contains(sourceVertex.dataSource))

                // filter the target vertices, removing any which are contained in excludeSets
                val filteredTargetDataSources: mutable.HashSet[DataSource] =
                  if (filteredExcludeSets.nonEmpty) {
                    targetDataSources.filterNot { targetDataSource: DataSource =>
                      filteredExcludeSets.exists { excludeSet: Set[DataSource] =>
                        if (excludeSet.contains(targetDataSource)) {
                          logWarning( // TODO: make this log optional && INFO level
                            s"excluding ${sourceVertex.dataSource.refString} --> " +
                              s"(${relationship.refString}) --> ${targetDataSource.refString}"
                          )
                          true // exclude this data source
                        } else {
                          false // include this data source
                        }
                      }
                    }
                  } else {
                    targetDataSources
                  }

                // loop over each target DataSource
                filteredTargetDataSources foreach { targetDataSource: DataSource =>
                  val targetId: Long = inverseVertexMap(targetDataSource)
                  val targetVertex: VertexData = vertexMap(targetId)

                  // get the relationship's join-key field-names for this target-vertex
                  val targetJoinKeyFieldNames: Set[String] =
                    relationship.getJoinKeyFieldNames(targetVertex.dataSource)

                  // get the groupBy field-names for this target-vertex, if any
                  //  - note, all non-root vertices will have a DataSourceConfig defined, so `.get` is safe
                  val targetGroupByFieldNames: Set[String] =
                    targetVertex.config.get.groupByFieldNames.toSet

                  // only continue with this target-vertex, if groupBy field-names is empty,
                  // or equal to the relationship's join-key field-names for this target-vertex
                  //  - note, this is because we can't join on columns inside the list generated by the groupBy
                  //  - note, this issue does not affect the source-vertex because the groupBy is
                  //    preformed AFTER joining all child vertices
                  if (
                    targetGroupByFieldNames.isEmpty ||
                    targetGroupByFieldNames.map(_.toLowerCase) ==
                      targetJoinKeyFieldNames.map(_.toLowerCase)
                  ) {

                    // add an edge from "source" -> "target"
                    graph.addEdge(sourceId, targetId)
                    edgeMap += (sourceId, targetId) -> EdgeData(relationship, relationshipConfig)

                    // update the visit count for this target-vertex at this `antiPriority`
                    visitedVertexCounter.update(
                      targetId,
                      visitedVertexCounter.getOrElseUpdate(
                        targetId,
                        mutable.ArrayBuffer()
                      ) += PathTriple(
                        sourceId = sourceId,
                        sourceName = sourceVertex.dataSource.refString,
                        relationshipName = relationship.refString,
                        targetId = targetId,
                        targetName = targetVertex.dataSource.refString
                      )
                    )
                  } else {
                    // TODO: make this log optional && INFO level
                    logWarning(
                      s"${relationship} can't be used to reach ${targetVertex.dataSource}: " +
                        s"groupBy fields [${targetGroupByFieldNames.mkString(", ")}] not identical (case-incentive) " +
                        s"to relationship joinKey fields [${targetJoinKeyFieldNames.mkString(", ")}], continuing..."
                    )
                  }
                }
              }
            }
          }
        }

        // after processing each relationship at this `antiPriority`, unpack how many times we visited each vertex
        visitedVertexCounter foreach {
          case (vertexId: Long, pathTriples: mutable.ArrayBuffer[PathTriple]) =>
            val vertexDataSource = vertexMap(vertexId).dataSource

            // there can't be multiple paths to a vertex at same `currentDepth`, with the same `antiPriority`
            if (pathTriples.size > 1) {
              val pathDescriptions: Seq[String] = pathTriples.map { path: PathTriple =>
                String.format(
                  " * %-" +
                    // calculate the maximum length of relationshipName, for padding
                    pathTriples
                      .maxBy(_.relationshipName.length)
                      .relationshipName
                      .length +
                    "s",
                  path.relationshipName
                ) +
                  s" - ${path.sourceName} (id #${path.sourceId}) --> ${path.targetName} (id #${path.targetId})"
              }
              throw new Exception( // TODO: custom exception object
                f"""found multiple equivalent paths @ depth=${currentDepth} with antiPriority=${antiPriority}:
                  |${pathDescriptions.mkString("\n")}
                  |tip: You can resolve this conflict by updating `antiPriority` or `exclusions`""".stripMargin +
                  "for one of these relationships"
              )
            }

            // we don't need to visit this vertex for any higher `antiPriority` levels
            //  - we must have found the lowest `antiPriority` path to this vertex already,
            //    because we loop in ascending `antiPriority` order
            unvisitedVertices -= vertexId
            unvisitedDataSources -= vertexDataSource

            // the vertices we traversed to during this iteration are the active-vertices for the next iteration
            nextActiveVertices += vertexId
        }
      }

      // update the active vertices
      activeVertices.clear()
      activeVertices ++= nextActiveVertices

      // increment current depth
      currentDepth += 1

      // don't allow `maxDepth` to be exceeded
      if (currentDepth > maxDepth) {
        throw new Exception( // TODO: custom exception object
          s"maxDepth exceeded"
        )
      }
    }

    // ----------------
    // Cleanup
    // ----------------
    unvisitedVertices.foreach { vertexId: Long =>
      val vertexData: VertexData = vertexMap(vertexId)

      // apply the 'pathMissingMode'
      vertexData.config.get.pathMissingMode match {
        case PathMissingMode.ERROR =>
          throw new Exception( // TODO: custom exception object
            s"${vertexData.dataSource.refString} was unused, and has pathMissingMode=ERROR"
          )
        case PathMissingMode.CONTINUE_WARN =>
          logWarning(
            s"${vertexData.dataSource.refString} was unused, and has pathMissingMode=CONTINUE_WARN"
          )
        case PathMissingMode.CONTINUE_IGNORE =>
      }

      // remove the vertex from the graph
      graph.removeVertex(vertexId)

      // remove the vertex from the vertexMap
      vertexMap -= vertexId
    }

    // TODO: remove after DEBUG
    logWarning("----------------------")

    new DataModelGraph(
      graph = graph,
      rootConfig = rootDataSourceConfig,
      vertexMap = vertexMap.toMap,
      edgeMap = edgeMap.toMap
    )
  }

  // ========================
  // Utility Methods
  // ========================
  /** A `sortWith` function which sorts strings having "data" first, followed by all others in Lexicographic order */
  private def dataFirstThenLexicographic: (String, String) => Boolean =
    (left: String, right: String) =>
      if (left == "data" && right != "data") {
        true // we want "data" first: therefore if "data" is on the left, leave it
      } else if (right == "data" && left != "data") {
        false // we want "data" first: therefore if "data" is on the right, swap it
      } else {
        left < right // in all other cases, use lexicographic order
      }

  // ========================
  // Classes
  // ========================
  /** Data associated with a graph vertex */
  case class VertexData(
      dataSource: DataSource,
      config: Option[OtherDataSourceConfig] = None
  )

  /** Data associated with a graph edge */
  case class EdgeData(
      relationship: DataRelationship,
      config: DataRelationshipConfig
  )

  /** A graph path-triple, used when logging multiple paths to the same vertex */
  private case class PathTriple(
      sourceId: Long,
      sourceName: String,
      relationshipName: String,
      targetId: Long,
      targetName: String
  )

  /** Configs related to a root data source */
  case class RootDataSourceConfig(
      snapshotFilter: RootSnapshotFilter
  )

  /** Configs related to a non-root data source */
  case class OtherDataSourceConfig(
      fieldName: String,
      pathMissingMode: PathMissingMode,
      joinFieldMissingMode: JoinFieldMissingMode,
      groupByFieldNames: Seq[String],
      groupByFieldMissingMode: GroupByFieldMissingMode,
      snapshotPreference: SnapshotPreference,
      snapshotMissingMode: SnapshotMissingMode,
      snapshotTimeBefore: TimeOffset,
      snapshotTimeAfter: TimeOffset
  )

  /** Configs related to a data relationship */
  case class DataRelationshipConfig(
      antiPriority: Long,
      excludeSets: Seq[Set[DataSource]]
  )

  // ========================
  // Exceptions
  // ========================
  /** An Exception which bubbles the fact we want to skip the current snapshot, but not fail the whole job */
  class SkipSnapshotException(message: String = "", cause: Throwable = None.orNull)
      extends Exception(message, cause)

}
