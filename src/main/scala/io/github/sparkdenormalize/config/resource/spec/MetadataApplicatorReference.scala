package io.github.sparkdenormalize.config.resource.spec

import io.circe.{Decoder, DecodingFailure, HCursor}

case class MetadataApplicatorReference(
    name: String
) {
  require(name.nonEmpty, "`name` must be non-empty")
}

object MetadataApplicatorReference {

  implicit val decodeMetadataApplicatorReference: Decoder[MetadataApplicatorReference] =
    new Decoder[MetadataApplicatorReference] {
      final def apply(c: HCursor): Decoder.Result[MetadataApplicatorReference] =
        for {
          name <- c.get[String]("name")
        } yield {
          try {
            MetadataApplicatorReference(name = name)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
