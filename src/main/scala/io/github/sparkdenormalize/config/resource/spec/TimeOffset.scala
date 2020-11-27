package io.github.sparkdenormalize.config.resource.spec

import io.circe.{Decoder, DecodingFailure, HCursor}

/** An time offset which can be applied to a date-time */
case class TimeOffset(
    nano: Long = 0L,
    second: Long = 0L,
    minute: Long = 0L,
    hour: Long = 0L,
    day: Long = 0L,
    week: Long = 0L,
    month: Long = 0L,
    year: Long = 0L
) {
  require(nano >= 0, s"`nanno` must be >= 0, but got: ${nano}")
  require(second >= 0, s"`second` must be >= 0, but got: ${second}")
  require(minute >= 0, s"`minute` must be >= 0, but got: ${minute}")
  require(hour >= 0, s"`hour` must be >= 0, but got: ${hour}")
  require(day >= 0, s"`day` must be >= 0, but got: ${day}")
  require(week >= 0, s"`week` must be >= 0, but got: ${week}")
  require(month >= 0, s"`month` must be >= 0, but got: ${month}")
  require(year >= 0, s"`year` must be >= 0, but got: ${year}")
}

object TimeOffset {

  implicit val decodeTimeOffset: Decoder[TimeOffset] =
    new Decoder[TimeOffset] {
      final def apply(c: HCursor): Decoder.Result[TimeOffset] =
        for {
          nano <- c.getOrElse[Long]("nano")(0L)
          second <- c.getOrElse[Long]("second")(0L)
          minute <- c.getOrElse[Long]("minute")(0L)
          hour <- c.getOrElse[Long]("hour")(0L)
          day <- c.getOrElse[Long]("day")(0L)
          week <- c.getOrElse[Long]("week")(0L)
          month <- c.getOrElse[Long]("month")(0L)
          year <- c.getOrElse[Long]("year")(0L)
        } yield {
          try {
            TimeOffset(
              nano = nano,
              second = second,
              minute = minute,
              hour = hour,
              day = day,
              week = week,
              month = month,
              year = year
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

}
