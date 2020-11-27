package io.github.sparkdenormalize.config.resource.spec.datasource

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.github.sparkdenormalize.config.resource.spec.{
  DataSourceReferenceWithAlias,
  MetadataApplicatorReference
}

/** A DataSource which is a 'view' of another DataSource */
case class SimpleVirtualDataSourceSpec(
    dataSource: DataSourceReferenceWithAlias,
    dataQuery: String,
    metadataApplicators: List[MetadataApplicatorReference] = List[MetadataApplicatorReference]()
) extends DataSourceSpec {
  require(dataQuery.nonEmpty, "`dataQuery` must be non-empty")
}

object SimpleVirtualDataSourceSpec {

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeSimpleVirtualDataSourceSpec: Decoder[SimpleVirtualDataSourceSpec] =
    new Decoder[SimpleVirtualDataSourceSpec] {
      final def apply(c: HCursor): Decoder.Result[SimpleVirtualDataSourceSpec] =
        for {
          dataSource <- c.get[DataSourceReferenceWithAlias]("dataSource")
          dataQuery <- c.get[String]("dataQuery")
          metadataApplicators <-
            c.getOrElse[List[MetadataApplicatorReference]]("metadataApplicators")(List())
        } yield {
          try {
            SimpleVirtualDataSourceSpec(
              dataSource = dataSource,
              dataQuery = dataQuery,
              metadataApplicators = metadataApplicators
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
