package io.github.sparkdenormalize.config.resource.spec

import java.net.URI
import java.time.{ZonedDateTime, ZoneId}

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveEnumerationDecoder}
import io.github.sparkdenormalize.config.resource.decoder.JavaNetURI.decodeURI
import io.github.sparkdenormalize.config.resource.decoder.JavaTime.{
  decodeZonedDateTime,
  decodeZoneId
}

/** An output dataset which combines data sources */
case class OutputDatasetSpec(
    dataModel: OutputDatasetSpec.DataModel,
    outputFolder: OutputDatasetSpec.OutputFolder
) extends ResourceSpec {
  // get output snapshot `fieldName`
  private val snapshotFieldName: Option[String] =
    outputFolder.snapshotFormat.hivePartitioned.map(_.fieldName)

  // ensure output snapshot `fieldName` is not used in `otherDataSources`
  snapshotFieldName.foreach { fieldName: String =>
    require(
      !dataModel.groupedFieldNames.contains(fieldName.toLowerCase),
      s"`spec.outputFolder.snapshotFormat.hivePartitioned.fieldName` cannot overlap " +
        s"with any `spec.dataModel.otherDataSources[].fieldName` (case-insensitive), but got: ${fieldName}"
    )
  }
}

// scalastyle:off number.of.types
// scalastyle:off number.of.methods
object OutputDatasetSpec {

  // field names which are not allowed (should be lower-case)
  val RESTRICTED_FIELD_NAMES = Set("data")

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[ZoneId]]
  implicitly[Decoder[ZonedDateTime]]
  implicitly[Decoder[URI]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeOutputDatasetSpec: Decoder[OutputDatasetSpec] =
    new Decoder[OutputDatasetSpec] {
      final def apply(c: HCursor): Decoder.Result[OutputDatasetSpec] =
        for {
          dataModel <- c.get[DataModel]("dataModel")
          outputFolder <- c.get[OutputFolder]("outputFolder")
        } yield {
          try {
            OutputDatasetSpec(
              dataModel = dataModel,
              outputFolder = outputFolder
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // --------
  // DataModel
  // --------
  implicit val decodeDataModel: Decoder[DataModel] =
    new Decoder[DataModel] {
      final def apply(c: HCursor): Decoder.Result[DataModel] =
        for {
          rootDataSource <- c.get[RootDataSource]("rootDataSource")
          otherDataSources <- c.get[List[OtherDataSource]]("otherDataSources")
          dataRelationships <- c.get[List[DataRelationship]]("dataRelationships")
          maxDepth <- c.getOrElse[Long]("maxDepth")(14L) // scalastyle:ignore magic.number
        } yield {
          try {
            DataModel(
              rootDataSource = rootDataSource,
              otherDataSources = otherDataSources,
              dataRelationships = dataRelationships,
              maxDepth = maxDepth
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // --------
  // DataModel.RootDataSource
  // --------
  implicit val decodeRootDataSource: Decoder[RootDataSource] =
    deriveConfiguredDecoder[RootDataSource]

  implicit val decodeSnapshotFilterConfig: Decoder[SnapshotFilterConfig] =
    new Decoder[SnapshotFilterConfig] {
      final def apply(c: HCursor): Decoder.Result[SnapshotFilterConfig] =
        for {
          fromInterval <- c.get[Option[SnapshotFilterFromInterval]]("fromInterval")
        } yield {
          try {
            SnapshotFilterConfig(fromInterval = fromInterval)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeSnapshotFilterFromInterval: Decoder[SnapshotFilterFromInterval] =
    deriveConfiguredDecoder[SnapshotFilterFromInterval]

  // --------
  // DataModel.OtherDataSource
  // --------
  implicit val decodeOtherDataSource: Decoder[OtherDataSource] =
    new Decoder[OtherDataSource] {
      final def apply(c: HCursor): Decoder.Result[OtherDataSource] =
        for {
          dataSource <- c.get[DataSourceReference]("dataSource")
          fieldName <- c.get[String]("fieldName")
          pathMissingMode <- c.getOrElse[PathMissingMode]("pathMissingMode")(PathMissingMode.ERROR)
          joinFieldMissingMode <- c.getOrElse[JoinFieldMissingMode]("joinFieldMissingMode")(
            JoinFieldMissingMode.ERROR
          )
          transformations <- c.getOrElse[OtherDataSourceTransformations]("transformations")(
            OtherDataSourceTransformations()
          )
          snapshot <- c.getOrElse[OtherDataSourceSnapshot]("snapshot")(OtherDataSourceSnapshot())
        } yield {
          try {
            OtherDataSource(
              dataSource = dataSource,
              fieldName = fieldName,
              pathMissingMode = pathMissingMode,
              joinFieldMissingMode = joinFieldMissingMode,
              transformations = transformations,
              snapshot = snapshot
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeOtherOtherDataSourceSnapshot: Decoder[OtherDataSourceSnapshot] =
    deriveConfiguredDecoder[OtherDataSourceSnapshot]

  implicit val decodeOtherDataSourceTransformations: Decoder[OtherDataSourceTransformations] =
    deriveConfiguredDecoder[OtherDataSourceTransformations]

  implicit val decodeOtherDataSourceTransformationsGroupBy
      : Decoder[OtherDataSourceTransformationsGroupBy] =
    new Decoder[OtherDataSourceTransformationsGroupBy] {
      final def apply(c: HCursor): Decoder.Result[OtherDataSourceTransformationsGroupBy] =
        for {
          fieldNames <- c.getOrElse[List[String]]("fieldNames")(List())
          groupByFieldMissingMode <-
            c.getOrElse[GroupByFieldMissingMode]("groupByFieldMissingMode")(
              GroupByFieldMissingMode.ERROR
            )
        } yield {
          try {
            OtherDataSourceTransformationsGroupBy(
              fieldNames = fieldNames,
              groupByFieldMissingMode = groupByFieldMissingMode
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // --------
  // DataModel.DataRelationship
  // --------
  implicit val decodeDataRelationship: Decoder[DataRelationship] =
    new Decoder[DataRelationship] {
      final def apply(c: HCursor): Decoder.Result[DataRelationship] =
        for {
          name <- c.get[String]("name")
          antiPriority <- c.getOrElse[Long]("antiPriority")(1L)
          exclusions <- c.getOrElse[List[DataRelationshipExclusions]]("exclusions")(List())
        } yield {
          try {
            DataRelationship(
              name = name,
              antiPriority = antiPriority,
              exclusions = exclusions
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeDataRelationshipExclusions: Decoder[DataRelationshipExclusions] =
    new Decoder[DataRelationshipExclusions] {
      final def apply(c: HCursor): Decoder.Result[DataRelationshipExclusions] =
        for {
          dataSourceSet <- c.get[Set[DataSourceReference]]("dataSourceSet")
        } yield {
          try {
            DataRelationshipExclusions(dataSourceSet = dataSourceSet)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // --------
  // OutputFolder
  // --------
  implicit val decodeOutputFolder: Decoder[OutputFolder] =
    new Decoder[OutputFolder] {
      final def apply(c: HCursor): Decoder.Result[OutputFolder] =
        for {
          folderUri <- c.get[URI]("folderUri")
          dataFormat <- c.getOrElse[OutputDataFormat]("dataFormat")(OutputDataFormat.PARQUET)
          dataCompression <- c.getOrElse[OutputDataCompression]("dataCompression")(
            OutputDataCompression.SNAPPY
          )
          snapshotCatchupMode <- c.getOrElse[OutputSnapshotCatchupMode]("snapshotCatchupMode")(
            OutputSnapshotCatchupMode.OLDEST_FIRST
          )
          snapshotFormat <- c.get[OutputSnapshotFormatConfig]("snapshotFormat")
          snapshotAutoDelete <- c.getOrElse[OutputSnapshotAutoDelete]("snapshotAutoDelete")(
            OutputSnapshotAutoDelete()
          )
        } yield {
          try {
            OutputFolder(
              folderUri = folderUri,
              dataFormat = dataFormat,
              dataCompression = dataCompression,
              snapshotCatchupMode = snapshotCatchupMode,
              snapshotFormat = snapshotFormat,
              snapshotAutoDelete = snapshotAutoDelete
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeOutputSnapshotFormatConfig: Decoder[OutputSnapshotFormatConfig] =
    new Decoder[OutputSnapshotFormatConfig] {
      final def apply(c: HCursor): Decoder.Result[OutputSnapshotFormatConfig] =
        for {
          hivePartitioned <- c.get[Option[OutputSnapshotFormatHivePartitioned]]("hivePartitioned")
        } yield {
          try {
            OutputSnapshotFormatConfig(hivePartitioned = hivePartitioned)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeOutputSnapshotFormatHivePartitioned
      : Decoder[OutputSnapshotFormatHivePartitioned] =
    new Decoder[OutputSnapshotFormatHivePartitioned] {
      final def apply(c: HCursor): Decoder.Result[OutputSnapshotFormatHivePartitioned] =
        for {
          fieldName <- c.getOrElse[String]("fieldName")("snapshot_epoch")
          timestampFormat <- c.getOrElse[OutputSnapshotTimestampFormat]("timestampFormat")(
            OutputSnapshotTimestampFormat.UNIX_MILLISECONDS
          )
          outputTimezone <- c.getOrElse[ZoneId]("outputTimezone")(ZoneId.of("UTC"))
        } yield {
          try {
            OutputSnapshotFormatHivePartitioned(
              fieldName = fieldName,
              timestampFormat = timestampFormat,
              outputTimezone = outputTimezone
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeOutputSnapshotAutoDelete: Decoder[OutputSnapshotAutoDelete] =
    new Decoder[OutputSnapshotAutoDelete] {
      final def apply(c: HCursor): Decoder.Result[OutputSnapshotAutoDelete] =
        for {
          maxSnapshotAge <- c.getOrElse[TimeOffset]("maxSnapshotAge")(TimeOffset())
          maxSnapshotCreationAge <- c.getOrElse[TimeOffset]("maxSnapshotCreationAge")(TimeOffset())
          maxOldSnapshots <- c.getOrElse[Long]("maxOldSnapshots")(-1L)
        } yield {
          try {
            OutputSnapshotAutoDelete(
              maxSnapshotAge = maxSnapshotAge,
              maxSnapshotCreationAge = maxSnapshotCreationAge,
              maxOldSnapshots = maxOldSnapshots
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // --------
  // ENUMS
  // --------
  implicit val decodePathMissingMode: Decoder[PathMissingMode] =
    deriveEnumerationDecoder[PathMissingMode]

  implicit val decodeJoinFieldMissingMode: Decoder[JoinFieldMissingMode] =
    deriveEnumerationDecoder[JoinFieldMissingMode]

  implicit val decodeGroupByFieldMissingMode: Decoder[GroupByFieldMissingMode] =
    deriveEnumerationDecoder[GroupByFieldMissingMode]

  implicit val decodeSnapshotPreference: Decoder[SnapshotPreference] =
    deriveEnumerationDecoder[SnapshotPreference]

  implicit val decodeSnapshotMissingMode: Decoder[SnapshotMissingMode] =
    deriveEnumerationDecoder[SnapshotMissingMode]

  implicit val decodeOutputDataFormat: Decoder[OutputDataFormat] =
    deriveEnumerationDecoder[OutputDataFormat]

  implicit val decodeOutputDataCompression: Decoder[OutputDataCompression] =
    deriveEnumerationDecoder[OutputDataCompression]

  implicit val decodeOutputSnapshotCatchupMode: Decoder[OutputSnapshotCatchupMode] =
    deriveEnumerationDecoder[OutputSnapshotCatchupMode]

  implicit val decodeOutputSnapshotTimestampFormat: Decoder[OutputSnapshotTimestampFormat] =
    deriveEnumerationDecoder[OutputSnapshotTimestampFormat]

  // ========================
  // Classes
  // ========================
  // --------
  // DataModel
  // --------
  case class DataModel(
      rootDataSource: RootDataSource,
      otherDataSources: List[OtherDataSource],
      dataRelationships: List[DataRelationship],
      maxDepth: Long // default: 14L
  ) {
    // collect a map of fieldName (used to check duplicates in outer layer)
    private[OutputDatasetSpec] val groupedFieldNames: Map[String, List[String]] = otherDataSources
      .map(_.fieldName)
      .groupBy(_.toLowerCase)

    // ensure `otherDataSources.fieldName` are unique (case-insensitive)
    groupedFieldNames.values.foreach { fieldNames: Seq[String] =>
      if (fieldNames.size > 1) {
        throw new IllegalArgumentException(
          s"`otherDataSources[].fieldName` must be unique (case-insensitive), but got: " +
            s"[${fieldNames.mkString(", ")}]"
        )
      }
    }

    // put sensible bounds on `maxDepth`
    require(
      maxDepth >= 1 && maxDepth <= 32,
      s"`maxDepth` must be between 1 and 32 (inclusive), but got: ${maxDepth}"
    )
  }

  // --------
  // DataModel.RootDataSource
  // --------
  case class RootDataSource(
      dataSource: DataSourceReference,
      snapshotFilter: SnapshotFilterConfig
  )

  case class SnapshotFilterConfig(
      fromInterval: Option[SnapshotFilterFromInterval]
  ) {
    // count the number of optional configs which were specified
    private val numSpecified: Long =
      Seq(fromInterval).foldLeft(0L) { case (total: Long, option: Option[_]) =>
        if (option.isDefined) {
          total + 1
        } else {
          total
        }
      }

    // ensure exactly one config is specified
    require(
      numSpecified == 1,
      s"`snapshotFilter` requires that exactly one config be specified, but got: ${numSpecified}"
    )
  }

  case class SnapshotFilterFromInterval(
      preference: SnapshotPreference = SnapshotPreference.NEWEST,
      intervalStartDate: ZonedDateTime,
      intervalLength: TimeOffset
  )

  // --------
  // DataModel.OtherDataSource
  // --------
  case class OtherDataSource(
      dataSource: DataSourceReference,
      fieldName: String,
      pathMissingMode: PathMissingMode, // default: PathMissingMode.ERROR
      joinFieldMissingMode: JoinFieldMissingMode, // default: JoinFieldMissingMode.ERROR
      transformations: OtherDataSourceTransformations, // default: OtherDataSourceTransformations()
      snapshot: OtherDataSourceSnapshot // default: OtherDataSourceSnapshot()
  ) {
    require(fieldName.nonEmpty, "`fieldName` must be non-empty")
    require(
      !RESTRICTED_FIELD_NAMES.contains(fieldName.toLowerCase),
      s"`fieldName` used a restricted value (case-insensitive): '${fieldName}'"
    )
  }

  case class OtherDataSourceSnapshot(
      preference: SnapshotPreference = SnapshotPreference.NEWEST,
      snapshotMissingMode: SnapshotMissingMode = SnapshotMissingMode.ERROR,
      timeBefore: TimeOffset = TimeOffset(),
      timeAfter: TimeOffset = TimeOffset()
  )

  case class OtherDataSourceTransformations(
      groupBy: Option[OtherDataSourceTransformationsGroupBy] = None
  )

  case class OtherDataSourceTransformationsGroupBy(
      fieldNames: List[String], // default: List()
      groupByFieldMissingMode: GroupByFieldMissingMode // default: GroupByFieldMissingMode.ERROR
  ) {
    require(fieldNames.forall(_.nonEmpty), "all `fieldNames` must be non-empty")
    require(
      fieldNames.size == fieldNames.map(_.toLowerCase).distinct.size,
      s"`fieldNames` must have no duplicates (case-insensitive), but got: [${fieldNames.mkString(", ")}]"
    )
  }

  // --------
  // DataModel.DataRelationship
  // --------
  case class DataRelationship(
      name: String,
      antiPriority: Long, // default: 1L
      exclusions: List[DataRelationshipExclusions] // default: List()
  ) {
    require(name.nonEmpty, "`name` must be non-empty")
    require(
      antiPriority >= 1 && antiPriority <= 1000000,
      s"`antiPriority` must be between 1 and 1,000,000 (inclusive), but got: ${antiPriority}"
    )
  }

  case class DataRelationshipExclusions(
      dataSourceSet: Set[DataSourceReference]
  ) {
    require(
      dataSourceSet.size >= 2,
      s"`dataSourceSet` must contain 2 or more data sources, but got: ${dataSourceSet.size}"
    )
  }

  // --------
  // OutputFolder
  // --------
  case class OutputFolder(
      folderUri: URI,
      dataFormat: OutputDataFormat, // default: OutputDataFormat.PARQUET
      dataCompression: OutputDataCompression, // default: OutputDataCompression.SNAPPY
      snapshotCatchupMode: OutputSnapshotCatchupMode, // default: OutputSnapshotCatchupMode.OLDEST_FIRST
      snapshotFormat: OutputSnapshotFormatConfig,
      snapshotAutoDelete: OutputSnapshotAutoDelete // default: OutputSnapshotAutoCleanup()
  ) {
    require(folderUri != new URI(""), "`folderUri` must be non-empty")
    require(
      folderUri.getPath.endsWith("/"),
      s"`folderUri` must end with '/', but got: ${folderUri}"
    )
    require(
      folderUri.isAbsolute,
      s"`folderUri` must be absolute (have a scheme defined), but got ${folderUri}"
    )
  }

  case class OutputSnapshotFormatConfig(
      hivePartitioned: Option[OutputSnapshotFormatHivePartitioned]
  ) {
    // count the number of optional configs which were specified
    private val numSpecified: Long =
      Seq(hivePartitioned).foldLeft(0L) { case (total: Long, option: Option[_]) =>
        if (option.isDefined) {
          total + 1
        } else {
          total
        }
      }

    // ensure exactly one config is specified
    require(
      numSpecified == 1,
      s"`snapshotFormat` requires that exactly one config be specified, but got: ${numSpecified}"
    )
  }

  case class OutputSnapshotFormatHivePartitioned(
      fieldName: String, // default: "snapshot_epoch"
      timestampFormat: OutputSnapshotTimestampFormat, // default: OutputSnapshotTimestampFormat.UNIX_MILLISECONDS
      outputTimezone: ZoneId // default: ZoneId.of("UTC")
  ) {
    require(fieldName.nonEmpty, "`fieldName` must be non-empty")
    require(
      !RESTRICTED_FIELD_NAMES.contains(fieldName.toLowerCase),
      s"`fieldName` used a restricted value (case-insensitive): '${fieldName}'"
    )
  }

  case class OutputSnapshotAutoDelete(
      maxSnapshotAge: TimeOffset = TimeOffset(), // default: TimeOffset()
      maxSnapshotCreationAge: TimeOffset = TimeOffset(), // default: TimeOffset()
      maxOldSnapshots: Long = -1L // default: -1L
  ) {
    require(
      maxOldSnapshots >= -1 && maxOldSnapshots <= 1000000000,
      s"`maxOldSnapshots` must be between -1 and 1,000,000,000 (inclusive), but got: ${maxOldSnapshots}"
    )
  }

  // ========================
  // Enums
  // ========================
  sealed trait PathMissingMode
  object PathMissingMode {
    // scalastyle:off object.name
    case object ERROR extends PathMissingMode
    case object CONTINUE_IGNORE extends PathMissingMode
    case object CONTINUE_WARN extends PathMissingMode
    // scalastyle:on object.name
  }

  sealed trait JoinFieldMissingMode
  object JoinFieldMissingMode {
    // scalastyle:off object.name
    case object ERROR extends JoinFieldMissingMode
    case object CONTINUE_IGNORE extends JoinFieldMissingMode
    case object CONTINUE_WARN extends JoinFieldMissingMode
    case object SKIP_IGNORE extends JoinFieldMissingMode
    case object SKIP_WARN extends JoinFieldMissingMode
    // scalastyle:on object.name
  }

  sealed trait GroupByFieldMissingMode
  object GroupByFieldMissingMode {
    // scalastyle:off object.name
    case object ERROR extends GroupByFieldMissingMode
    case object CONTINUE_IGNORE extends GroupByFieldMissingMode
    case object CONTINUE_WARN extends GroupByFieldMissingMode
    case object SKIP_IGNORE extends GroupByFieldMissingMode
    case object SKIP_WARN extends GroupByFieldMissingMode
    // scalastyle:on object.name
  }

  sealed trait SnapshotPreference
  object SnapshotPreference {
    // scalastyle:off object.name
    case object NEWEST extends SnapshotPreference
    case object OLDEST extends SnapshotPreference
    // scalastyle:on object.name
  }

  sealed trait SnapshotMissingMode
  object SnapshotMissingMode {
    // scalastyle:off object.name
    case object ERROR extends SnapshotMissingMode
    case object CONTINUE_IGNORE extends SnapshotMissingMode
    case object CONTINUE_WARN extends SnapshotMissingMode
    case object SKIP_IGNORE extends SnapshotMissingMode
    case object SKIP_WARN extends SnapshotMissingMode
    // scalastyle:on object.name
  }

  sealed trait OutputDataFormat
  object OutputDataFormat {
    // scalastyle:off object.name
    case object PARQUET extends OutputDataFormat
    case object AVRO extends OutputDataFormat
    case object JSONL extends OutputDataFormat
    // scalastyle:on object.name
  }

  sealed trait OutputDataCompression
  object OutputDataCompression {
    // scalastyle:off object.name
    case object SNAPPY extends OutputDataCompression
    case object NONE extends OutputDataCompression
    // scalastyle:on object.name
  }

  sealed trait OutputSnapshotTimestampFormat
  object OutputSnapshotTimestampFormat {
    // scalastyle:off object.name
    case object ISO8601_DATE_TIME extends OutputSnapshotTimestampFormat
    case object UNIX_MILLISECONDS extends OutputSnapshotTimestampFormat
    case object UNIX_SECONDS extends OutputSnapshotTimestampFormat
    // scalastyle:on object.name
  }

  sealed trait OutputSnapshotCatchupMode
  object OutputSnapshotCatchupMode {
    // scalastyle:off object.name
    case object OLDEST_FIRST extends OutputSnapshotCatchupMode
    case object NEWEST_FIRST extends OutputSnapshotCatchupMode
    // scalastyle:on object.name
  }

}
