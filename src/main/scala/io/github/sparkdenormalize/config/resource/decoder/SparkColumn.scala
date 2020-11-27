package io.github.sparkdenormalize.config.resource.decoder

import io.circe.Decoder
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

object SparkColumn {

  implicit val decodeColumn: Decoder[Column] =
    Decoder.decodeString.emap { exprString: String =>
      try {
        Right(expr(exprString))
      } catch {
        case ex: Exception =>
          Left(ex.getMessage)
      }
    }

}
