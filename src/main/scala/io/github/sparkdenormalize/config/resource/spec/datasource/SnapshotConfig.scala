package io.github.sparkdenormalize.config.resource.spec.datasource

import java.time.ZoneId

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveEnumerationDecoder}

case class SnapshotConfig(
    fromFieldValue: Option[SnapshotConfig.SnapshotFromFieldValue] = None, // default: None
    fromReadTime: Option[SnapshotConfig.SnapshotFromReadTime] = None // default: None
) {
  // count the number of optional configs which were specified
  private val numSpecified: Long =
    Seq(fromFieldValue, fromReadTime).foldLeft(0L) { case (total: Long, option: Option[_]) =>
      if (option.isDefined) {
        total + 1
      } else {
        total
      }
    }

  // ensure exactly one config is specified
  require(
    numSpecified == 1,
    s"`snapshot` requires that exactly one config be specified, but got: ${numSpecified}"
  )
}

object SnapshotConfig {

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[ZoneId]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeSnapshotConfig: Decoder[SnapshotConfig] =
    new Decoder[SnapshotConfig] {
      final def apply(c: HCursor): Decoder.Result[SnapshotConfig] =
        for {
          fromFieldValue <- c.get[Option[SnapshotFromFieldValue]]("fromFieldValue")
          fromReadTime <- c.get[Option[SnapshotFromReadTime]]("fromReadTime")
        } yield {
          try {
            SnapshotConfig(fromFieldValue = fromFieldValue, fromReadTime = fromReadTime)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeSnapshotFromFieldValue: Decoder[SnapshotFromFieldValue] =
    new Decoder[SnapshotFromFieldValue] {
      final def apply(c: HCursor): Decoder.Result[SnapshotFromFieldValue] =
        for {
          fieldName <- c.get[String]("fieldName")
          timeFormat <- c.get[SnapshotTimeFormat]("timeFormat")
          defaultTimezone <- c.getOrElse[ZoneId]("defaultTimezone")(ZoneId.of("UTC"))
          dropField <- c.getOrElse[Boolean]("dropField")(true)
        } yield {
          try {
            SnapshotFromFieldValue(
              fieldName = fieldName,
              timeFormat = timeFormat,
              defaultTimezone = defaultTimezone,
              dropField = dropField
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeSnapshotFromReadTime: Decoder[SnapshotFromReadTime] =
    deriveConfiguredDecoder[SnapshotFromReadTime]

  implicit val decodeSnapshotTimeFormat: Decoder[SnapshotTimeFormat] =
    deriveEnumerationDecoder[SnapshotTimeFormat]

  // ========================
  // Classes
  // ========================
  case class SnapshotFromFieldValue(
      fieldName: String,
      timeFormat: SnapshotTimeFormat,
      defaultTimezone: ZoneId, // default: ZoneId.of("UTC")
      dropField: Boolean // default: true
  ) {
    require(fieldName.nonEmpty, "`fieldName` must be non-empty")
  }

  case class SnapshotFromReadTime()

  // ========================
  // Enums
  // ========================
  sealed trait SnapshotTimeFormat
  object SnapshotTimeFormat {
    // scalastyle:off object.name
    case object ISO8601_DATE extends SnapshotTimeFormat
    case object ISO8601_DATE_TIME extends SnapshotTimeFormat
    case object TIMESTAMP extends SnapshotTimeFormat
    case object UNIX_MILLISECONDS extends SnapshotTimeFormat
    case object UNIX_SECONDS extends SnapshotTimeFormat
    // scalastyle:on object.name
  }

}
