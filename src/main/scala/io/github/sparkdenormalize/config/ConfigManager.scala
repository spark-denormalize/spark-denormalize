package io.github.sparkdenormalize.config

import java.io.InputStreamReader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.{JacksonYAMLParseException, YAMLFactory}
import io.circe.{parser, Decoder, DecodingFailure, Json, ParsingFailure}
import io.github.sparkdenormalize.config.ConfigManager.safeAppend
import io.github.sparkdenormalize.config.resource.{Resource, ResourceKinds}
import io.github.sparkdenormalize.config.resource.spec._
import io.github.sparkdenormalize.config.resource.spec.datasource._
import io.github.sparkdenormalize.core.IOManager
import io.github.sparkdenormalize.core.datasource._
import io.github.sparkdenormalize.core.metadata.MetadataApplicator
import io.github.sparkdenormalize.core.output.OutputDataset
import io.github.sparkdenormalize.core.relationship.DataRelationship
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/** Global store for all config objects */
class ConfigManager private ()(implicit ss: SparkSession) extends Logging {

  // after a ConfigManager is finished, no more ResourceSpecs can be added to it
  private var finished: Boolean = false

  // ResourceSpecs
  private val _ComplexVirtualDataSourceSpecs =
    mutable.HashMap[String, ComplexVirtualDataSourceSpec]()
  private val _HDFSLikeDataSourceSpecs = mutable.HashMap[String, HDFSLikeDataSourceSpec]()
  private val _JDBCDataSourceSpecs = mutable.HashMap[String, JDBCDataSourceSpec]()
  private val _SimpleVirtualDataSourceSpecs = mutable.HashMap[String, SimpleVirtualDataSourceSpec]()
  private val _DataRelationshipSpecs = mutable.HashMap[String, DataRelationshipSpec]()
  private val _MetadataApplicatorSpecs = mutable.HashMap[String, MetadataApplicatorSpec]()
  private val _OutputDatasetSpecs = mutable.HashMap[String, OutputDatasetSpec]()

  // Resources
  private val _ComplexVirtualDataSources = mutable.HashMap[String, ComplexVirtualDataSource]()
  private val _HDFSLikeDataSources = mutable.HashMap[String, HDFSLikeDataSource]()
  private val _JDBCDataSources = mutable.HashMap[String, JDBCDataSource]()
  private val _SimpleVirtualDataSources = mutable.HashMap[String, SimpleVirtualDataSource]()
  private val _DataRelationships = mutable.HashMap[String, DataRelationship]()
  private val _MetadataApplicators = mutable.HashMap[String, MetadataApplicator]()
  private val _OutputDatasets = mutable.HashMap[String, OutputDataset]()

  // Implicits needed by Resource class builders
  implicit private val config: ConfigManager = this
  implicit private val io: IOManager = new IOManager()

  // ========================
  // Constructor Methods
  // ========================
  /** Mark this ConfigManager as finished */
  private def finish(): Unit = {
    assert(!finished, "ConfigManager has already been finished")

    // we add the Resources in the following order, so that Resources which depend on other Resources can
    // guarantee that all their dependencies are already in the ConfigManager

    // 0 - MetadataApplicators
    _MetadataApplicatorSpecs.foreach { case (name, spec) =>
      addMetadataApplicator(name, spec)
    }

    // 1 - DataSources
    _HDFSLikeDataSourceSpecs.foreach { case (name, spec) =>
      addHDFSLikeDataSource(name, spec)
    }
    _JDBCDataSourceSpecs.foreach { case (name, spec) =>
      addJDBCDataSource(name, spec)
    }

    // 2 - VirtualDataSources
    _SimpleVirtualDataSourceSpecs.foreach { case (name, spec) =>
      addSimpleVirtualDataSource(name, spec)
    }
    _ComplexVirtualDataSourceSpecs.foreach { case (name, spec) =>
      addComplexVirtualDataSource(name, spec)
    }

    // 3 - DataRelationships
    _DataRelationshipSpecs.foreach { case (name, spec) =>
      addDataRelationship(name, spec)
    }

    // 4 - OutputDatasets
    _OutputDatasetSpecs.foreach { case (name, spec) =>
      addOutputDataset(name, spec)
    }

    // mark this ConfigManager as finished
    finished = true
  }

  /** Add a Resource to this ConfigManager */
  private def addResourceSpec(name: String, spec: ResourceSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    spec match {
      // DataSources
      case s: ComplexVirtualDataSourceSpec => safeAppend(_ComplexVirtualDataSourceSpecs, name, s)
      case s: HDFSLikeDataSourceSpec       => safeAppend(_HDFSLikeDataSourceSpecs, name, s)
      case s: JDBCDataSourceSpec           => safeAppend(_JDBCDataSourceSpecs, name, s)
      case s: SimpleVirtualDataSourceSpec  => safeAppend(_SimpleVirtualDataSourceSpecs, name, s)
      // DataRelationships
      case s: DataRelationshipSpec => safeAppend(_DataRelationshipSpecs, name, s)
      // MetadataApplicators
      case s: MetadataApplicatorSpec => safeAppend(_MetadataApplicatorSpecs, name, s)
      // OutputDataset
      case s: OutputDatasetSpec => safeAppend(_OutputDatasetSpecs, name, s)
      // error
      case s: ResourceSpec =>
        throw new IllegalArgumentException(
          s"got unexpected ResourceSpec of type: ${s.getClass}"
        )
    }
  }

  /** Add a ComplexVirtualDataSource to this ConfigManager */
  private def addComplexVirtualDataSource(
      name: String,
      spec: ComplexVirtualDataSourceSpec
  ): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_ComplexVirtualDataSources, name, ComplexVirtualDataSource(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a HDFSLikeDataSource to this ConfigManager */
  private def addHDFSLikeDataSource(name: String, spec: HDFSLikeDataSourceSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_HDFSLikeDataSources, name, HDFSLikeDataSource(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a JDBCDataSource to this ConfigManager */
  private def addJDBCDataSource(name: String, spec: JDBCDataSourceSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_JDBCDataSources, name, JDBCDataSource(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a SimpleVirtualDataSourceSpec to this ConfigManager */
  private def addSimpleVirtualDataSource(name: String, spec: SimpleVirtualDataSourceSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_SimpleVirtualDataSources, name, SimpleVirtualDataSource(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a DataRelationship to this ConfigManager */
  private def addDataRelationship(name: String, spec: DataRelationshipSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_DataRelationships, name, DataRelationship(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a MetadataApplicator to this ConfigManager */
  private def addMetadataApplicator(name: String, spec: MetadataApplicatorSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_MetadataApplicators, name, MetadataApplicator(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  /** Add a OutputDataset to this ConfigManager */
  private def addOutputDataset(name: String, spec: OutputDatasetSpec): Unit = {
    assert(!finished, "ConfigManager has already been finished")
    safeAppend(_OutputDatasets, name, OutputDataset(name, spec))

    // TODO: remove after test
    logWarning(s"${spec.getClass.getSimpleName}/${name} || ${spec}")
  }

  // ========================
  // Getters
  // ========================
  /** Get all OutputDatasets stored in this ConfigManager */
  def allOutputDataset(): Seq[OutputDataset] =
    _OutputDatasets.values.toSeq

  /** Get a DataSource from this ConfigManager
    *
    * @param reference the DataSourceReference
    * @return the DataSource
    * @throws IllegalArgumentException if the ConfigManager does not contain `reference`
    */
  def getDataSource(reference: DataSourceReference): DataSource = {
    safeGetDataSource(reference) match {
      case Some(dataSource: DataSource) => dataSource
      case None =>
        throw new IllegalArgumentException(
          s"ConfigManager does not contain: ${reference.kind}/${reference.name}"
        )
    }
  }

  /** Safely get a DataSource from this ConfigManager, if any
    *
    * @param reference the DataSourceReference
    * @return the DataSource, if any
    */
  def safeGetDataSource(reference: DataSourceReference): Option[DataSource] = {
    reference.kind match {
      case ResourceKinds.ComplexVirtualDataSource => _ComplexVirtualDataSources.get(reference.name)
      case ResourceKinds.HDFSLikeDataSource       => _HDFSLikeDataSources.get(reference.name)
      case ResourceKinds.JDBCDataSource           => _JDBCDataSources.get(reference.name)
      case ResourceKinds.SimpleVirtualDataSource  => _SimpleVirtualDataSources.get(reference.name)
    }
  }

  /** Get a DataRelationship from this ConfigManager
    *
    * @param name the name of the DataRelationship
    * @return the DataRelationship
    * @throws IllegalArgumentException if the ConfigManager does not contain `name`
    */
  def getDataRelationship(name: String): DataRelationship = {
    safeGetDataRelationship(name) match {
      case Some(dataRelationship: DataRelationship) => dataRelationship
      case None =>
        throw new IllegalArgumentException(
          s"ConfigManager does not contain: DataRelationship/${name}"
        )
    }
  }

  /** Safely get a DataRelationship from this ConfigManager, if any
    *
    * @param name the name of the DataRelationship
    * @return the DataRelationship, if any
    */
  def safeGetDataRelationship(name: String): Option[DataRelationship] =
    _DataRelationships.get(name)

}

object ConfigManager {

  /** Construct a ConfigManager from a sequence of file paths */
  def apply(filePaths: Seq[String])(implicit ss: SparkSession): ConfigManager = {
    val config = new ConfigManager()
    for (path <- filePaths) {
      val jsonStrings: Seq[String] = readYamlFileAsJson(path)
      val resources: Seq[Resource] = jsonStrings.map { s: String =>
        parseResourceJson(s, path)
      }
      resources.foreach { r: Resource =>
        try {
          config.addResourceSpec(r.metadata.name, r.spec)
        } catch {
          case ex: Exception =>
            throw new Exception( // TODO: custom exception object
              s"error while parsing config file at: ${path}",
              ex
            )
        }
      }
    }
    config.finish()
    config
  }

  /** Read a YAML file from disk as a sequence of JSON strings.
    *
    * NOTE:
    *  - if the YAML file contain multiple objects (separated by "---"),
    *    a separate JSON string will be returned for each
    *
    * @param filePath the path of the file
    * @return a sequence of JSON strings
    *  @throws Exception if a parsing error is encountered while processing the YAML
    *  @throws Exception if an unhandled error is encountered while reading the file
    */
  private def readYamlFileAsJson(filePath: String): Seq[String] = {
    val yamlFactory = new YAMLFactory()
    val yamlReader = new ObjectMapper(yamlFactory)
    val jsonWriter = new ObjectMapper()

    val reader: InputStreamReader = Source.fromFile(filePath).reader()
    try {
      yamlReader
        .readValues(yamlFactory.createParser(reader), classOf[Any])
        .asScala
        .toSeq
        .map(jsonWriter.writeValueAsString)
    } catch {
      case ex: JacksonYAMLParseException =>
        throw new Exception( // TODO: custom exception object
          s"error parsing YAML from file: ${filePath}",
          ex
        )
      case ex: Exception =>
        throw new Exception( // TODO: custom exception object
          s"error reading file: ${filePath}",
          ex
        )
    } finally {
      reader.close()
    }
  }

  /** Parse a JSON string into a spark-denormalize config Resource
    *
    * @param jsonString the json string
    * @param filePath   the path of the file which contains the JSON, (used in error text to help users)
    * @return a spark-denormalize config Resource
    * @throws ParsingFailure if the string is not valid JSON
    * @throws Exception      if an unhandled error is encountered while decoding the JSON
    * @throws Exception      if an decoding error is encountered while decoding the JSON
    */
  private def parseResourceJson(jsonString: String, filePath: String): Resource = {

    val json: Json = parser.parse(jsonString) match {
      case Right(value: Json)       => value
      case Left(ex: ParsingFailure) => throw ex
    }

    // we wrap this in a try, because there might be cases where our Decoder code,
    // does not capture an error and bubble it as a DecodingFailure
    val resource: Decoder.Result[Resource] =
      try {
        json.as[Resource]
      } catch {
        case ex: Exception =>
          throw new Exception( // TODO: custom exception object
            s"""unhandled exception while decoding JSON into Resource
             |
             |------------
             |INFO
             |------------
             |filePath: ${filePath}
             |
             |------------
             |JSON
             |------------
             |${json.spaces2}
             |""".stripMargin,
            ex
          )
      }

    // unpack the decoder result
    resource match {
      case Right(value: Resource) => value
      case Left(ex: DecodingFailure) =>
        throw new Exception( // TODO: custom exception object
          s"""provided JSON is not a valid Resource
             |
             |------------
             |INFO
             |------------
             |filePath:      ${filePath}
             |errorPosition: ${ex.history.reverse.mkString(" --> ")}
             |error:         ${ex.message}
             |
             |------------
             |JSON
             |------------
             |${json.spaces2}
             |""".stripMargin
        )
    }
  }

  /** Safely append a key/value to a mutable map, throwing an error if the key already exists.
    *
    * @param map the map to mutate
    * @param key the key to append to the map
    * @param value the value to append to the map
    * @tparam K the type of the key
    * @tparam V the type of value
    * @throws IllegalArgumentException if the key is already in the map
    */
  private def safeAppend[K, V](map: mutable.HashMap[K, V], key: K, value: V): Unit = {
    val valueClassName = value.getClass.getSimpleName
    if (map.contains(key)) {
      throw new IllegalArgumentException(s"there is already a ${valueClassName} with name '${key}'")
    }
    map += key -> value
  }
}
