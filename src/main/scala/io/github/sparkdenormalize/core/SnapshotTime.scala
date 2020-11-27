package io.github.sparkdenormalize.core

import java.time.{Instant, ZonedDateTime, ZoneId}

import io.github.sparkdenormalize.config.resource.spec.TimeOffset
import io.github.sparkdenormalize.core.SnapshotTime.EpochType

/** A class storing the time-instant of some data snapshot */
class SnapshotTime private (protected val time: ZonedDateTime) extends Ordered[SnapshotTime] {

  def canEqual(a: Any): Boolean = a.isInstanceOf[SnapshotTime]

  override def compare(that: SnapshotTime): Int =
    this.time compareTo that.time

  override def equals(that: Any): Boolean =
    that match {
      case that: SnapshotTime => {
        that.canEqual(this) &&
        this.time.equals(that.time)
      }
      case _ => false
    }

  override def hashCode: Int = time.hashCode()

  override def toString: String = time.toString

  /** Get the value of this SnapshotTime as an epoch
    *
    * @param epochType the type of epoch
    * @return the value of this SnapshotTime
    */
  def asEpoch(epochType: EpochType = EpochType.UNIX_MILLISECONDS): Long = {
    epochType match {
      case EpochType.UNIX_MILLISECONDS => time.toInstant.toEpochMilli
      case EpochType.UNIX_SECONDS      => time.toEpochSecond
    }
  }

  /** Get the value of this SnapshotTime as a java Instant */
  def asInstant: Instant = {
    time.toInstant
  }

  /** Return a new SnapshotTime by adding a TimeOffset */
  def plus(offset: TimeOffset): SnapshotTime = {
    new SnapshotTime(
      time
        .plusYears(offset.year)
        .plusMonths(offset.month)
        .plusWeeks(offset.week)
        .plusDays(offset.day)
        .plusHours(offset.hour)
        .plusMinutes(offset.minute)
        .plusSeconds(offset.second)
        .plusNanos(offset.nano)
    )
  }

  /** Return a new SnapshotTime by subtracting a TimeOffset */
  def minus(offset: TimeOffset): SnapshotTime = {
    new SnapshotTime(
      time
        .minusYears(offset.year)
        .minusMonths(offset.month)
        .minusWeeks(offset.week)
        .minusDays(offset.day)
        .minusHours(offset.hour)
        .minusMinutes(offset.minute)
        .minusSeconds(offset.second)
        .minusNanos(offset.nano)
    )
  }

}

object SnapshotTime {

  /** Build a SnapshotTime from an epoch
    *
    * @param epoch the long containing the epoch
    * @param epochType the type of epoch contained in `epoch`
    * @return the SnapshotTime
    */
  def apply(epoch: Long, epochType: EpochType = EpochType.UNIX_MILLISECONDS): SnapshotTime = {
    val instant: Instant = epochType match {
      case EpochType.UNIX_MILLISECONDS => Instant.ofEpochMilli(epoch)
      case EpochType.UNIX_SECONDS      => Instant.ofEpochSecond(epoch)
    }
    val time = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
    new SnapshotTime(time)
  }

  sealed trait EpochType
  object EpochType {
    // scalastyle:off object.name
    case object UNIX_MILLISECONDS extends EpochType
    case object UNIX_SECONDS extends EpochType
    // scalastyle:on object.name
  }

}
