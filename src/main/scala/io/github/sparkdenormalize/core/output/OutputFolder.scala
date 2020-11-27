package io.github.sparkdenormalize.core.output

import java.net.URI
import java.time.ZoneId

import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.{
  OutputDataCompression,
  OutputDataFormat,
  OutputSnapshotCatchupMode,
  OutputSnapshotTimestampFormat
}
import io.github.sparkdenormalize.config.resource.spec.TimeOffset
import io.github.sparkdenormalize.core.{IOManager, SnapshotTime}
import org.apache.spark.sql.types.StructType

class OutputFolder private (
    uri: URI,
    dataConfig: OutputFolder.DataConfig,
    snapshotConfig: OutputFolder.SnapshotConfig
)(implicit io: IOManager) {

  // a function to sort SnapshotTimes into the order they should be committed (depends on catchup mode)
  val snapshotTimeSortFunction: (SnapshotTime, SnapshotTime) => Boolean = {
    snapshotConfig.catchupMode match {
      case OutputSnapshotCatchupMode.OLDEST_FIRST =>
        (left: SnapshotTime, right: SnapshotTime) => left < right
      case OutputSnapshotCatchupMode.NEWEST_FIRST =>
        (left: SnapshotTime, right: SnapshotTime) => right < left
    }
  }

  def writeSnapshot(snapshot: OutputSnapshot): Unit = {
    // TODO: validate OutputFolder is empty or correct
    //  - !! we should validate OutputFolder is empty/correct as we write (rather than on init)

    // TODO: folder structure
    //  - ./aranui.json
    //     ~ specifies 'version' of aranui format (could be used in future for different folder types/structures)
    //  - ./data/
    //     ~ whatever data format is specified by `snapshotFormat` and `dataFormat`
    //  - ./metadata/
    //     - ./manifest.json
    //        ~ logs committed snapshots
    //        ~ mapping from snapshot -> schema
    //     - ./schema/
    //        - ./{SCHEMA_HASH}.json
    //           ~ stores schema
    //           ~ ?? will column metadata be stored here (or somewhere else to reduce unnecessary changes)
    //     - ./datamodel/
    //        - ./merged.json
    //           ~ a merged view of the data model (used to detect DataSources moving around tree)
    //        - ./{JOB_RUN_TIME}.json
    //           ~ datamodelgraph as at {JOB_RUN_TIME}
    //           ~ obviously if current time is behind newest {JOB_RUN_TIME}, fail

    // TODO: ?? do we want to include info about why a field is null in the metadata
    //  (and allow users to easily find snapshots which have non-null fields for some requested set of data sources)

    // TODO: create config in OutputFolder for what to do if asked to write incompatible schema
    //  - options: FAIL, WARN, (+ something clever, specific to aranui format)
    //  - ?? how do we help the user solve the issue if they end up in a fail

    // TODO: handle locks to prevent multiple writers at the same time
    //  - ?? what is needed
    //  - !! look at delta-lake to see how they allow this
    ???
  }

  /** Retrieve the set of snapshots which have already been loaded into this OutputFolder */
  def committedSnapshots(): Set[SnapshotTime] = {

    // TODO: can this be optimized for consumption by returning exclude ranges?
    ???
  }

  /** Retrieve the data schema at a specific snapshot-time, if any
    *
    * @param snapshot the snapshot time
    * @return the data schema, if any
    */
  def schemaAtSnapshot(snapshot: SnapshotTime): Option[StructType] = {
    ???
  }

  /** Retrieve the data model at a specific snapshot-time, if any
    *
    * @param snapshot the snapshot time
    * @return the data model graph, if any
    */
  def dataModelAtSnapshot(snapshot: SnapshotTime): Option[DataModelGraph] = {
    ???
  }

}

object OutputFolder {

  /** Construct an OutputFolder */
  def apply(uri: URI, dataConfig: DataConfig, snapshotConfig: SnapshotConfig)(implicit
      io: IOManager
  ): OutputFolder = {
    new OutputFolder(uri, dataConfig, snapshotConfig)
  }

  // ========================
  // Classes
  // ========================
  case class DataConfig(
      dataFormat: OutputDataFormat,
      dataCompression: OutputDataCompression
  )

  case class SnapshotConfig(
      catchupMode: OutputSnapshotCatchupMode,
      snapshotFormat: SnapshotFormat,
      maxSnapshotAge: TimeOffset,
      maxSnapshotCreationAge: TimeOffset,
      maxOldSnapshots: Long
  )

  sealed trait SnapshotFormat
  case class HivePartitionedSnapshotFormat(
      fieldName: String,
      timestampFormat: OutputSnapshotTimestampFormat,
      outputTimezone: ZoneId
  ) extends SnapshotFormat

}
