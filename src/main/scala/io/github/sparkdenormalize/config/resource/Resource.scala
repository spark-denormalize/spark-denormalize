package io.github.sparkdenormalize.config.resource

import io.circe._
import io.github.sparkdenormalize.config.resource.ResourceKinds._
import io.github.sparkdenormalize.config.resource.spec._
import io.github.sparkdenormalize.config.resource.spec.datasource._

/** The root config class */
case class Resource(
    apiVersion: String,
    kind: ResourceKind,
    metadata: ResourceMetadata,
    spec: ResourceSpec
)

object Resource {

  implicit val decodeResource: Decoder[Resource] =
    new Decoder[Resource] {
      final def apply(c: HCursor): Decoder.Result[Resource] = {

        // decode + unpack the `apiVersion` field
        val apiVersion: String =
          c.get[String]("apiVersion") match {
            case Right(value: String)      => value
            case Left(ex: DecodingFailure) => return Left(ex) // scalastyle:ignore return
          }

        // decode + unpack the `kind` field
        val kind: ResourceKind =
          c.get[ResourceKind]("kind") match {
            case Right(value: ResourceKind) => value
            case Left(ex: DecodingFailure)  => return Left(ex) // scalastyle:ignore return
          }

        // decode + unpack the `metadata` field
        val metadata: ResourceMetadata =
          c.get[ResourceMetadata]("metadata") match {
            case Right(value: ResourceMetadata) => value
            case Left(ex: DecodingFailure)      => return Left(ex) // scalastyle:ignore return
          }

        // decode the `spec` field
        val specResult: Decoder.Result[ResourceSpec] =
          (apiVersion, kind) match {
            // DataSources
            case ("v1", ComplexVirtualDataSource) => c.get[ComplexVirtualDataSourceSpec]("spec")
            case ("v1", HDFSLikeDataSource)       => c.get[HDFSLikeDataSourceSpec]("spec")
            case ("v1", JDBCDataSource)           => c.get[JDBCDataSourceSpec]("spec")
            case ("v1", SimpleVirtualDataSource)  => c.get[SimpleVirtualDataSourceSpec]("spec")
            // DataRelationships
            case ("v1", DataRelationship) => c.get[DataRelationshipSpec]("spec")
            // MetadataApplicators
            case ("v1", MetadataApplicator) => c.get[MetadataApplicatorSpec]("spec")
            // OutputDataset
            case ("v1", OutputDataset) => c.get[OutputDatasetSpec]("spec")
            // error
            case (_, _) =>
              Left(
                DecodingFailure(
                  s"`${apiVersion}/${kind}` is not a supported resource kind",
                  c.history
                )
              )
          }

        // unpack the `spec` field
        val spec: ResourceSpec =
          specResult match {
            case Right(value: ResourceSpec) => value
            case Left(ex: DecodingFailure)  => return Left(ex) // scalastyle:ignore return
          }

        Right(Resource(apiVersion, kind, metadata, spec))

      }
    }

}
