package io.github.sparkdenormalize.config.resource.spec.datasource

import java.net.URI

import io.circe._
import io.github.sparkdenormalize.config.resource.decoder.JavaNetURI.decodeURI
import io.github.sparkdenormalize.config.resource.decoder.SparkDataType.decodeStructField
import io.github.sparkdenormalize.config.resource.spec.MetadataApplicatorReference
import org.apache.spark.sql.types.StructField

/** A DataSource which is HDFS-like (e.g. HDFS/S3/GCS) */
case class HDFSLikeDataSourceSpec(
    format: String,
    formatOptions: Map[String, String], // default: Map()
    sourceUriGlobs: List[String],
    sourceUriPrefix: Option[String], // TODO: custom class for sourceUriPrefix
    snapshot: SnapshotConfig,
    schemaFields: Option[List[StructField]],
    metadataApplicators: List[MetadataApplicatorReference] // default: List()
) extends DataSourceSpec {
  require(format.nonEmpty, "`format` must be non-empty")
  require(sourceUriGlobs.nonEmpty, "`sourceUriGlobs` must be non-empty")
  require(sourceUriPrefix.exists(_.nonEmpty), "if specified, `sourceUriPrefix` must be non-empty")
}

object HDFSLikeDataSourceSpec {

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[StructField]]
  implicitly[Decoder[URI]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeHDFSLikeDataSourceSpec: Decoder[HDFSLikeDataSourceSpec] =
    new Decoder[HDFSLikeDataSourceSpec] {
      final def apply(c: HCursor): Decoder.Result[HDFSLikeDataSourceSpec] =
        for {
          format <- c.get[String]("format")
          formatOptions <- c.getOrElse[Map[String, String]]("formatOptions")(Map())
          sourceUriGlobs <- c.get[List[String]]("sourceUriGlobs")
          sourceUriPrefix <- c.get[Option[String]]("sourceUriPrefix")
          snapshot <- c.get[SnapshotConfig]("snapshot")
          schemaFields <- c.get[Option[List[StructField]]]("schemaFields")
          metadataApplicators <-
            c.getOrElse[List[MetadataApplicatorReference]]("metadataApplicators")(List())
        } yield {
          try {
            HDFSLikeDataSourceSpec(
              format = format,
              formatOptions = formatOptions,
              sourceUriGlobs = sourceUriGlobs,
              sourceUriPrefix = sourceUriPrefix,
              snapshot = snapshot,
              schemaFields = schemaFields,
              metadataApplicators = metadataApplicators
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
