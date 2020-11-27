package io.github.sparkdenormalize.core.output

import java.time.ZonedDateTime

import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.SnapshotPreference
import io.github.sparkdenormalize.config.resource.spec.TimeOffset

sealed trait RootSnapshotFilter {}

object RootSnapshotFilter {

  case class FromInterval(
      preference: SnapshotPreference,
      intervalStartDate: ZonedDateTime,
      intervalLength: TimeOffset
  ) extends RootSnapshotFilter

}
