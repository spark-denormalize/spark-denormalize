package io.github.sparkdenormalize.core.output

import io.github.sparkdenormalize.config.ConfigManager
import io.github.sparkdenormalize.config.resource.spec.{DataSourceReference, OutputDatasetSpec}
import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.GroupByFieldMissingMode
import io.github.sparkdenormalize.core.{IOManager, Resource, SnapshotTime}
import io.github.sparkdenormalize.core.datasource.DataSource
import io.github.sparkdenormalize.core.output.DataModelGraph.{
  DataRelationshipConfig,
  OtherDataSourceConfig,
  RootDataSourceConfig,
  SkipSnapshotException
}
import io.github.sparkdenormalize.core.output.OutputFolder.{
  DataConfig,
  HivePartitionedSnapshotFormat,
  SnapshotConfig
}
import io.github.sparkdenormalize.core.relationship.DataRelationship
import io.github.sparkdenormalize.spark.{LeftJoinListener, MetricListener}
import org.apache.spark.sql.SparkSession

class OutputDataset private (val name: String, dataModel: DataModelGraph, folder: OutputFolder)
    extends Resource {

  /** Update the OutputFolder, if anything is to be done */
  def sync(): Unit = {
    // TODO: validate that DataModelGraph has not changed incompatibly (from the stored union on disk)
    //  - ensure graph data-model structure has not changed (data sources cannot move around)
    //     - ensure that fieldName is treated case-insensitive
    //  - ensure groupBy fields have not changed

    // TODO: validate OutputFolder has not changed incompatibly
    //  - outputFolder.dataFormat
    //  - outputFolder.dataCompression

    // TODO: ?? should some validations be done earlier to cause fast-failure
    //  - !! Main calls sync() sequentially

    val snapshotTimes: Seq[SnapshotTime] = missingSnapshotTimes()
    snapshotTimes.foreach { snapshotTime: SnapshotTime =>
      try {
        val outputSnapshot: OutputSnapshot = dataModel.readSnapshot(snapshotTime)
        folder.writeSnapshot(outputSnapshot)
      } catch {
        case ex: SkipSnapshotException =>
          // TODO: both SKIP_WARN & SKIP_IGNORE will throw here, should we still log?
          ???
        case ex: Exception =>
          // TODO: enrich the Exception with information about what we called, and re-throw
          ???
      }

    }
  }

  /** Calculate the snapshots which are missing from the OutputFolder */
  private def missingSnapshotTimes(): Seq[SnapshotTime] = {
    // TODO: we need to be careful that this method is not called to early,
    //       as it depends on the current time

    // configs which alter behaviour:
    //  - OutputDataset.spec.outputFolder.snapshotAutoDelete.maxSnapshotAge
    //  - OutputDataset.spec.outputFolder.snapshotAutoDelete.maxSnapshotCreationAge
    //  - OutputDataset.spec.outputFolder.snapshotAutoDelete.maxOldSnapshots
    //  - the TBA abandon age config after which we will not even look at if a snapshot has been made
    //     - note this config is different to maxSnapshotAge because we wont delete existing snapshots

    // TODO: improve this method
    val excludeSingle: Set[SnapshotTime] = folder.committedSnapshots()

    val missingSet: Set[SnapshotTime] = dataModel.availableRootSnapshotTimes(
      excludeAfter = ???,
      excludeBefore = ???,
      excludeSingle = ???,
      excludeRanges = ???
    )
    missingSet.toSeq.sortWith(folder.snapshotTimeSortFunction)
  }

}

object OutputDataset {

  /** Build a OutputDataset from a OutputDatasetSpec */
  def apply(name: String, spec: OutputDatasetSpec)(implicit
      config: ConfigManager,
      io: IOManager,
      ss: SparkSession
  ): OutputDataset = {

    // ----------------
    // DataModelGraph
    // ----------------
    val rootDataSource: DataSource =
      config.getDataSource(spec.dataModel.rootDataSource.dataSource)

    val rootDataSourceConfig: RootDataSourceConfig = {
      // in future there may be alternatives to `fromInterval`,
      // but for now we can safely assume `fromInterval` option will be nonEmpty
      val fromIntervalSpec = spec.dataModel.rootDataSource.snapshotFilter.fromInterval.get
      RootDataSourceConfig(
        snapshotFilter = RootSnapshotFilter.FromInterval(
          preference = fromIntervalSpec.preference,
          intervalStartDate = fromIntervalSpec.intervalStartDate,
          intervalLength = fromIntervalSpec.intervalLength
        )
      )
    }

    val otherDataSources: Seq[(DataSource, OtherDataSourceConfig)] =
      spec.dataModel.otherDataSources.map { odsSpec: OutputDatasetSpec.OtherDataSource =>
        (
          config.getDataSource(odsSpec.dataSource),
          OtherDataSourceConfig(
            fieldName = odsSpec.fieldName,
            pathMissingMode = odsSpec.pathMissingMode,
            joinFieldMissingMode = odsSpec.joinFieldMissingMode,
            groupByFieldNames = odsSpec.transformations.groupBy
              .map(_.fieldNames)
              .getOrElse(List()),
            groupByFieldMissingMode = odsSpec.transformations.groupBy
              .map(_.groupByFieldMissingMode)
              .getOrElse(GroupByFieldMissingMode.ERROR),
            snapshotPreference = odsSpec.snapshot.preference,
            snapshotMissingMode = odsSpec.snapshot.snapshotMissingMode,
            snapshotTimeBefore = odsSpec.snapshot.timeBefore,
            snapshotTimeAfter = odsSpec.snapshot.timeAfter
          )
        )
      }
    val dataRelationships: Seq[(DataRelationship, DataRelationshipConfig)] =
      spec.dataModel.dataRelationships.map { relSpec: OutputDatasetSpec.DataRelationship =>
        (
          config.getDataRelationship(relSpec.name),
          DataRelationshipConfig(
            antiPriority = relSpec.antiPriority,
            excludeSets = relSpec.exclusions.map(_.dataSourceSet.map(config.getDataSource))
          )
        )
      }
    val graph =
      try {
        DataModelGraph(
          rootDataSource = rootDataSource,
          rootDataSourceConfig = rootDataSourceConfig,
          otherDataSources = otherDataSources,
          dataRelationships = dataRelationships,
          maxDepth = spec.dataModel.maxDepth
        )
      } catch {
        case ex: Exception =>
          throw new Exception( // TODO: custom exception object
            s"OutputDataset/${name} can't be instantiated because of an error in its DataModelGraph",
            ex
          )
      }

    // ----------------
    // OutputFolder
    // ----------------
    val dataConfig: DataConfig = DataConfig(
      dataFormat = spec.outputFolder.dataFormat,
      dataCompression = spec.outputFolder.dataCompression
    )

    val snapshotConfig: SnapshotConfig = {
      // in future there may be alternatives to `hivePartitioned`,
      // but for now we can safely assume `hivePartitioned` option will be nonEmpty
      val hivePartitionedSpec = spec.outputFolder.snapshotFormat.hivePartitioned.get
      SnapshotConfig(
        catchupMode = spec.outputFolder.snapshotCatchupMode,
        snapshotFormat = HivePartitionedSnapshotFormat(
          fieldName = hivePartitionedSpec.fieldName,
          timestampFormat = hivePartitionedSpec.timestampFormat,
          outputTimezone = hivePartitionedSpec.outputTimezone
        ),
        maxSnapshotAge = spec.outputFolder.snapshotAutoDelete.maxSnapshotAge,
        maxSnapshotCreationAge = spec.outputFolder.snapshotAutoDelete.maxSnapshotCreationAge,
        maxOldSnapshots = spec.outputFolder.snapshotAutoDelete.maxOldSnapshots
      )
    }

    val folder =
      try {
        OutputFolder(
          uri = spec.outputFolder.folderUri,
          dataConfig = dataConfig,
          snapshotConfig = snapshotConfig
        )
      } catch {
        case ex: Exception =>
          throw new Exception( // TODO: custom exception object
            s"OutputDataset/${name} can't be instantiated because of an error in its OutputFolder",
            ex
          )
      }

    new OutputDataset(name, graph, folder)
  }

}
