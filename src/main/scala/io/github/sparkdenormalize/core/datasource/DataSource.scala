package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.SnapshotPreference
import io.github.sparkdenormalize.core.{Resource, SnapshotTime}
import org.apache.spark.sql.DataFrame

trait DataSource extends Resource {

  // TODO: `bestAvailableSnapshot`
  //  - ?? can KTable can be queried through JDBC
  //     - how would users specify snapshot extract in JDBCDataSource
  //  - make special SnapshotTime for current time

  /** Get the best available snapshot from the DataSource, given the constraints */
  def bestAvailableSnapshot(
      snapshotPreference: SnapshotPreference,
      minSnapshotTime: SnapshotTime,
      maxSnapshotTime: SnapshotTime
  ): Option[SnapshotTime] = {
    // TODO: make abstract, to ensure all DataSources have implemented
    ???
  }

  /** Read this DataSource at a specific SnapshotTime */
  def readAt(snapshotTime: SnapshotTime): DataFrame = {
    // TODO: make abstract, to ensure all DataSources have implemented
    ???
  }

}
