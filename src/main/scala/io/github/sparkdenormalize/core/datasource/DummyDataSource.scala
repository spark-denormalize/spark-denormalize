package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec
import io.github.sparkdenormalize.core.SnapshotTime
import org.apache.spark.sql.DataFrame

class DummyDataSource private (val name: String, data: DataFrame, hasSnapshot: Boolean)
    extends DataSource {

  /** Read this DataSource at a specific SnapshotTime */
  override def readAt(snapshotTime: SnapshotTime): DataFrame = {
    // just return the wrapped DataFrame for Dummy
    data
  }

  /** Get the best available snapshot from the DataSource, given the constraints */
  override def bestAvailableSnapshot(
      snapshotPreference: OutputDatasetSpec.SnapshotPreference,
      minSnapshotTime: SnapshotTime,
      maxSnapshotTime: SnapshotTime
  ): Option[SnapshotTime] = {
    if (hasSnapshot) {
      // just return any snapshot for Dummy
      Some(SnapshotTime(epoch = 0))
    } else {
      None
    }
  }
}

object DummyDataSource {

  /** Build a DummyDataSource from a Spark DataFrame */
  def apply(name: String, dataFrame: DataFrame, hasSnapshot: Boolean = true): DummyDataSource = {
    // TODO: implement this
    new DummyDataSource(name, dataFrame, hasSnapshot)
  }

}
