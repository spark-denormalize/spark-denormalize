package io.github.sparkdenormalize.config.resource.spec.datasource

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.github.sparkdenormalize.config.resource.spec.{
  DataSourceReferenceWithAlias,
  MetadataApplicatorReference
}

/** A DataSource which is a 'view' of multiple other DataSources */
case class ComplexVirtualDataSourceSpec(
    dataSources: List[DataSourceReferenceWithAlias],
    dataQuery: String,
    snapshotQuery: String,
    metadataApplicators: List[MetadataApplicatorReference] // default: List()
) extends DataSourceSpec {
  require(dataSources.nonEmpty, "`dataSources` must be non-empty")
  require(dataQuery.nonEmpty, "`dataQuery` must be non-empty")
  require(snapshotQuery.nonEmpty, "`snapshotQuery` must be non-empty")
  // TODO: validate `dataQuery` contains "{{ SNAPSHOT | XXX }}"
}

object ComplexVirtualDataSourceSpec {

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeComplexVirtualDataSourceSpec: Decoder[ComplexVirtualDataSourceSpec] =
    new Decoder[ComplexVirtualDataSourceSpec] {
      final def apply(c: HCursor): Decoder.Result[ComplexVirtualDataSourceSpec] =
        for {
          dataSources <- c.get[List[DataSourceReferenceWithAlias]]("dataSources")
          dataQuery <- c.get[String]("dataQuery")
          snapshotQuery <- c.get[String]("snapshotQuery")
          metadataApplicators <-
            c.getOrElse[List[MetadataApplicatorReference]]("metadataApplicators")(List())
        } yield {
          try {
            ComplexVirtualDataSourceSpec(
              dataSources = dataSources,
              dataQuery = dataQuery,
              snapshotQuery = snapshotQuery,
              metadataApplicators = metadataApplicators
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
