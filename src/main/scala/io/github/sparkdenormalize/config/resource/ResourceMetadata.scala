package io.github.sparkdenormalize.config.resource

import io.circe.{Decoder, DecodingFailure, HCursor}

/** Standard Resource metadata */
case class ResourceMetadata(
    name: String
) {
  require(name.nonEmpty, "`name` must be non-empty")
}

object ResourceMetadata {

  implicit val decodeResourceMetadata: Decoder[ResourceMetadata] =
    new Decoder[ResourceMetadata] {
      final def apply(c: HCursor): Decoder.Result[ResourceMetadata] =
        for {
          name <- c.get[String]("name")
        } yield {
          try {
            ResourceMetadata(name = name)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
