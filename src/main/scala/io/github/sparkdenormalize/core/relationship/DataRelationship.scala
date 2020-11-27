package io.github.sparkdenormalize.core.relationship

import scala.collection.mutable

import io.github.sparkdenormalize.config.ConfigManager
import io.github.sparkdenormalize.config.resource.spec.{DataRelationshipSpec, DataSourceReference}
import io.github.sparkdenormalize.config.resource.spec.DataRelationshipSpec.{
  RelationshipJoinKeyComponent,
  Representation
}
import io.github.sparkdenormalize.core.Resource
import io.github.sparkdenormalize.core.datasource.DataSource
import io.github.sparkdenormalize.core.relationship.DataRelationship.JoinKeyComponent
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

class DataRelationship private (
    val name: String,
    dataSourceMap: Map[DataSource, Seq[JoinKeyComponent]]
) extends Resource {

  // set view of DataSources in this relationship, to prevent recalculating
  private val dataSourceSet: mutable.HashSet[DataSource] =
    mutable.HashSet[DataSource](dataSourceMap.keys.toSeq: _*)

  /** Get the components of the join-key for a DataSource
    *
    * @param dataSource the DataSource
    * @return a sequence of JoinKeyComponent, which comprise the join-key
    * @throws IllegalArgumentException if the DataSource is not present in this relationship
    */
  def getJoinKey(dataSource: DataSource): Seq[JoinKeyComponent] = {
    require(
      dataSourceMap.contains(dataSource),
      s"${dataSource.refString} is not present in ${this.refString}"
    )
    dataSourceMap(dataSource)
  }

//  /** Get the Columns of the join-key for a DataSource
//    *
//    * @param dataSource the DataSource
//    * @return a sequence of unresolved spark Column, which comprise the join-key
//    * @throws IllegalArgumentException if the DataSource is not present in this relationship
//    */
//  def getJoinKeyColumns(dataSource: DataSource): Seq[Column] = {
//    require(
//      dataSourceMap.contains(dataSource),
//      s"${dataSource.refString} is not present in ${this.refString}"
//    )
//    dataSourceMap(dataSource)
//  }

  /** Get the set of field-names which are used as inputs to the join-key for a DataSource
    *
    * @param dataSource the DataSource
    * @return a set of field-name strings, which are used as inputs to the join-key
    * @throws IllegalArgumentException if the DataSource is not present in this relationship
    */
  def getJoinKeyFieldNames(dataSource: DataSource): Set[String] = {
    val joinKey: Seq[JoinKeyComponent] = getJoinKey(dataSource)
    joinKey.flatMap(_.fieldsMap.keys).toSet
  }

  /** Check if a data source is contained in this relationship */
  def hasDataSource(dataSource: DataSource): Boolean = {
    this.dataSourceSet.contains(dataSource)
  }

  /** Get the intersection of a mutable of data sources, and those contained in this relationship */
  def intersectMutable(that: mutable.HashSet[DataSource]): mutable.HashSet[DataSource] = {
    this.dataSourceSet intersect that
  }

}

object DataRelationship {

  /** Build a DataRelationship from a DataRelationshipSpec */
  def apply(name: String, spec: DataRelationshipSpec)(implicit
      config: ConfigManager
  ): DataRelationship = {
    val dataSourceJoinKeyMap = mutable.HashMap[DataSource, Seq[JoinKeyComponent]]()

    spec.representations.foreach { representation: Representation =>
      // convert joinKey into a sequence of unresolved spark Column
      val joinKey: Seq[JoinKeyComponent] = representation.joinKey.map {
        component: RelationshipJoinKeyComponent =>
          JoinKeyComponent(
            fieldsMap = component.fields.map(f => f.fieldName -> f.dataType).toMap,
            query = component.query,
            castDataType = component.castDataType
          )
      }

      // associate each DataSource in this Representation with the joinKey
      representation.dataSources.foreach { reference: DataSourceReference =>
        config.safeGetDataSource(reference) match {
          case Some(dataSource: DataSource) =>
            if (dataSourceJoinKeyMap.contains(dataSource)) {
              throw new IllegalArgumentException(
                s"DataRelationship/${name} contains ${dataSource.refString} multiple times"
              )
            }
            dataSourceJoinKeyMap += dataSource -> joinKey
          case None =>
            throw new IllegalArgumentException(
              s"DataRelationship/${name} contains ${reference.kind}/${reference.name}, which is not defined"
            )
        }
      }
    }
    new DataRelationship(name, dataSourceJoinKeyMap.toMap)
  }

  /** FOR TESTING: Build a DataRelationship from a Map */
  def apply(
      name: String,
      dataSourceJoinKeyMap: Map[DataSource, Seq[JoinKeyComponent]]
  ): DataRelationship = {
    new DataRelationship(name, dataSourceJoinKeyMap)
  }

  // ========================
  // Classes
  // ========================
  case class JoinKeyComponent(
      fieldsMap: Map[String, DataType],
      query: Column,
      castDataType: DataType
  )

}
