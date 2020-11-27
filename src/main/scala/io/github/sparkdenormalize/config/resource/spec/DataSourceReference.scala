package io.github.sparkdenormalize.config.resource.spec

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.github.sparkdenormalize.config.resource.ResourceKinds.DataSourceKind

case class DataSourceReference(
    kind: DataSourceKind,
    name: String
) {
  require(name.nonEmpty, "`name` must be non-empty")
}

object DataSourceReference {

  implicit val decodeDataSourceReference: Decoder[DataSourceReference] =
    new Decoder[DataSourceReference] {
      final def apply(c: HCursor): Decoder.Result[DataSourceReference] =
        for {
          kind <- c.get[DataSourceKind]("kind")
          name <- c.get[String]("name")
        } yield {
          try {
            DataSourceReference(kind = kind, name = name)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }
}
