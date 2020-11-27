package io.github.sparkdenormalize.config.resource.decoder

import java.time.{ZonedDateTime, ZoneId}

import io.circe.{Decoder, DecodingFailure, HCursor}

object JavaTime {

  implicit val decodeZonedDateTime: Decoder[ZonedDateTime] =
    new Decoder[ZonedDateTime] {
      final def apply(c: HCursor): Decoder.Result[ZonedDateTime] =
        for {
          year <- c.get[Int]("year")
          month <- c.get[Int]("month")
          day <- c.get[Int]("day")
          hour <- c.get[Int]("hour")
          minute <- c.get[Int]("minute")
          second <- c.get[Int]("second")
          nano <- c.get[Int]("nano")
          timezone <- c.get[ZoneId]("timezone")
        } yield {
          try {
            ZonedDateTime.of(year, month, day, hour, minute, second, nano, timezone)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeZoneId: Decoder[ZoneId] =
    Decoder.decodeString.emap { zoneId: String =>
      try {
        Right(ZoneId.of(zoneId))
      } catch {
        case ex: Exception =>
          Left(ex.getMessage)
      }
    }
}
