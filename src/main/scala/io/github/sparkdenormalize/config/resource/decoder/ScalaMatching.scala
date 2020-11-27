package io.github.sparkdenormalize.config.resource.decoder

import scala.util.matching.Regex

import io.circe.Decoder

object ScalaMatching {

  implicit val decodeRegex: Decoder[Regex] =
    Decoder.decodeString.emap { regexString: String =>
      try {
        Right(regexString.r)
      } catch {
        case ex: Exception =>
          Left(ex.getMessage)
      }
    }

}
