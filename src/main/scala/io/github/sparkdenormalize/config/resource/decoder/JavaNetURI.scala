package io.github.sparkdenormalize.config.resource.decoder

import java.net.URI

import io.circe.Decoder

object JavaNetURI {

  implicit val decodeURI: Decoder[URI] =
    Decoder.decodeString.emap { uriString: String =>
      try {
        Right(new URI(uriString))
      } catch {
        case ex: Exception =>
          Left(ex.getMessage)
      }
    }
}
