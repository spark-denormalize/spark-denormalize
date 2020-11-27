package io.github.sparkdenormalize.config.resource.spec

import scala.util.matching.Regex

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder
import io.github.sparkdenormalize.config.resource.decoder.ScalaMatching.decodeRegex
import io.github.sparkdenormalize.config.resource.decoder.SparkDataType.decodeMetadata
import org.apache.spark.sql.types.Metadata

/** A definition of how to apply metadata to a DataSource */
case class MetadataApplicatorSpec(
    behaviour: MetadataApplicatorSpec.MetadataApplicatorBehaviour, // default: MetadataApplicatorBehaviour.MERGE
    fieldSelectors: List[MetadataApplicatorSpec.FieldSelector],
    metadata: Metadata
) extends ResourceSpec {
  require(fieldSelectors.nonEmpty, "`fieldSelectors` must be non-empty")
}

object MetadataApplicatorSpec {

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[Regex]]
  implicitly[Decoder[Metadata]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeMetadataApplicatorSpec: Decoder[MetadataApplicatorSpec] =
    new Decoder[MetadataApplicatorSpec] {
      final def apply(c: HCursor): Decoder.Result[MetadataApplicatorSpec] =
        for {
          behaviour <-
            c.getOrElse[MetadataApplicatorBehaviour]("behaviour")(MetadataApplicatorBehaviour.MERGE)
          fieldSelectors <- c.get[List[MetadataApplicatorSpec.FieldSelector]]("fieldSelectors")
          metadata <- c.get[Metadata]("metadata")
        } yield {
          try {
            MetadataApplicatorSpec(
              behaviour = behaviour,
              fieldSelectors = fieldSelectors,
              metadata = metadata
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeFieldSelector: Decoder[FieldSelector] =
    new Decoder[FieldSelector] {
      final def apply(c: HCursor): Decoder.Result[FieldSelector] =
        for {
          coordinateIncludeRegex <- c.get[Option[Regex]]("coordinateIncludeRegex")
          coordinateExcludeRegex <- c.get[Option[Regex]]("coordinateExcludeRegex")
        } yield {
          try {
            FieldSelector(
              coordinateIncludeRegex = coordinateIncludeRegex,
              coordinateExcludeRegex = coordinateExcludeRegex
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeMetadataApplicatorBehaviour: Decoder[MetadataApplicatorBehaviour] =
    deriveEnumerationDecoder[MetadataApplicatorBehaviour]

  // ========================
  // Classes
  // ========================
  case class FieldSelector(
      coordinateIncludeRegex: Option[Regex],
      coordinateExcludeRegex: Option[Regex]
  ) {
    // count the number of optional configs which were specified
    private val numSpecified: Long =
      Seq(coordinateIncludeRegex, coordinateExcludeRegex).foldLeft(0L) {
        case (total: Long, option: Option[_]) =>
          if (option.isDefined) {
            total + 1
          } else {
            total
          }
      }

    // ensure at least one config is specified
    require(
      numSpecified >= 1,
      s"`fieldSelector` requires that at least one config be specified, but got: ${numSpecified}"
    )

    coordinateIncludeRegex.map(regex =>
      require(
        regex.pattern.pattern.nonEmpty,
        "if specified, `coordinateIncludeRegex` must be non-empty"
      )
    )
    coordinateExcludeRegex.map(regex =>
      require(
        regex.pattern.pattern.nonEmpty,
        "if specified, `coordinateExcludeRegex` must be non-empty"
      )
    )
  }

  // ========================
  // Enums
  // ========================
  sealed trait MetadataApplicatorBehaviour
  object MetadataApplicatorBehaviour {
    // scalastyle:off object.name
    case object IGNORE extends MetadataApplicatorBehaviour
    case object MERGE extends MetadataApplicatorBehaviour
    case object REPLACE extends MetadataApplicatorBehaviour
    // scalastyle:on object.name
  }

}
