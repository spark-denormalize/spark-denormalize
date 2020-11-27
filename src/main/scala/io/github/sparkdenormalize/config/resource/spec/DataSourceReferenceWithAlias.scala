package io.github.sparkdenormalize.config.resource.spec

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.github.sparkdenormalize.config.resource.ResourceKinds.DataSourceKind

case class DataSourceReferenceWithAlias(
    kind: DataSourceKind,
    name: String,
    alias: String
) {
  require(name.nonEmpty, "`name` must be non-empty")
  require(alias.nonEmpty, "`name` must be non-empty")
}

object DataSourceReferenceWithAlias {

  implicit val decodeDataSourceReferenceWithAlias: Decoder[DataSourceReferenceWithAlias] =
    new Decoder[DataSourceReferenceWithAlias] {
      final def apply(c: HCursor): Decoder.Result[DataSourceReferenceWithAlias] =
        for {
          kind <- c.get[DataSourceKind]("kind")
          name <- c.get[String]("name")
          alias <- c.get[String]("alias")
        } yield {
          try {
            DataSourceReferenceWithAlias(kind = kind, name = name, alias = alias)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }
}
