package io.github.sparkdenormalize.core.output

import io.github.sparkdenormalize.core.SnapshotTime
import org.apache.spark.sql.DataFrame

/** A wrapper for a specific instance of actual snapshot data */
class OutputSnapshot private (data: DataFrame, time: SnapshotTime) {

  /** Read the data stored in this snapshot */
  def read(): DataFrame = data

}

object OutputSnapshot {

  /** Build an OutputSnapshot */
  def apply(data: DataFrame, snapshotTime: SnapshotTime): OutputSnapshot = {
    new OutputSnapshot(data = data, time = snapshotTime)
  }

}
