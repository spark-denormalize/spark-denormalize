package io.github.sparkdenormalize.core.output

import java.time.{ZonedDateTime, ZoneId}

import io.github.sparkdenormalize.{CustomerData, SharedSparkSession, UnitSpec}
import io.github.sparkdenormalize.config.resource.spec.OutputDatasetSpec.{
  GroupByFieldMissingMode,
  JoinFieldMissingMode,
  PathMissingMode,
  SnapshotMissingMode,
  SnapshotPreference
}
import io.github.sparkdenormalize.config.resource.spec.TimeOffset
import io.github.sparkdenormalize.core.SnapshotTime
import io.github.sparkdenormalize.core.output.DataModelGraph.{
  DataRelationshipConfig,
  OtherDataSourceConfig,
  RootDataSourceConfig
}

// scalastyle:off magic.number
class DataModelGraphTest extends UnitSpec with SharedSparkSession {

  // ----------------
  // SHARED VALUES
  // ----------------
  val data = new CustomerData()
  val defaultRootDataSourceConfig = RootDataSourceConfig(
    snapshotFilter = RootSnapshotFilter.FromInterval(
      preference = SnapshotPreference.NEWEST,
      intervalStartDate = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")),
      intervalLength = TimeOffset(day = 1)
    )
  )
  val defaultOtherDataSourceConfig = OtherDataSourceConfig(
    fieldName = "__MISSING__",
    pathMissingMode = PathMissingMode.ERROR,
    joinFieldMissingMode = JoinFieldMissingMode.ERROR,
    groupByFieldNames = Seq(),
    groupByFieldMissingMode = GroupByFieldMissingMode.ERROR,
    snapshotPreference = SnapshotPreference.NEWEST,
    snapshotMissingMode = SnapshotMissingMode.ERROR,
    snapshotTimeBefore = TimeOffset(hour = 12),
    snapshotTimeAfter = TimeOffset(hour = 12)
  )
  val defaultDataRelationshipConfig = DataRelationshipConfig(
    antiPriority = 0,
    excludeSets = Seq()
  )

  // ----------------
  // TESTS
  // ----------------
  behavior of "DataModelGraph"

  it should "successfully .apply() with CustomerData" in {
    val rootDataSource = data.customerDataSource
    val otherDataSources = Seq(
      (
        data.customerAddressDataSource,
        defaultOtherDataSourceConfig.copy(fieldName = "customer_address")
      ),
      (
        data.customerGroupDataSource,
        defaultOtherDataSourceConfig
          .copy(fieldName = "customer_group", groupByFieldNames = Seq("customer_id"))
      ),
      (
        data.groupDetailsDataSource,
        defaultOtherDataSourceConfig.copy(fieldName = "group_details")
      )
    )
    val dataRelationships = Seq(
      (data.customerIdDataRelationship, defaultDataRelationshipConfig),
      (data.groupIdDataRelationship, defaultDataRelationshipConfig)
    )

    val dataModel = DataModelGraph(
      rootDataSource = rootDataSource,
      rootDataSourceConfig = defaultRootDataSourceConfig,
      otherDataSources = otherDataSources,
      dataRelationships = dataRelationships,
      maxDepth = 14 // scalastyle:off magic.number
    )

    val outputDotString: String = dataModel.asDotString(maxLabelWidth = 100)
    val expectedDotString: String =
      """strict digraph G {
        |  0 [ label="DummyDataSource/customer
        |groupBy: N/A" ];
        |  1 [ label="DummyDataSource/customer_address
        |groupBy: List()" ];
        |  2 [ label="DummyDataSource/customer_group
        |groupBy: List(customer_id)" ];
        |  3 [ label="DummyDataSource/group_details
        |groupBy: List()" ];
        |  0 -> 1 [ label="DataRelationship/customer_id
        |antiPriority: 0" ];
        |  0 -> 2 [ label="DataRelationship/customer_id
        |antiPriority: 0" ];
        |  2 -> 3 [ label="DataRelationship/group_id
        |antiPriority: 0" ];
        |}
        |""".stripMargin

    // TODO: it seems like the order of the edges in the DOT string is not deterministic
    //  - we need to fix this so we can correctly ensure the output is as expected

//    info(
//      s"""string representation of graph:
//         |---- GraphViz DOT ----
//         |${outputDotString}
//         |----------------------""".stripMargin
//    )

//    // write to disk (for graph-viz)
//    new PrintWriter("hack/data_model.dot") {
//      write(outputDotString); close()
//    }

    // check - output graph is as expected
    assertResult(expectedDotString)(outputDotString)
  }

  it should "successfully .readSnapshot() with CustomerData" in {
    val rootDataSource = data.customerDataSource
    val otherDataSources = Seq(
      (
        data.customerAddressDataSource,
        defaultOtherDataSourceConfig.copy(fieldName = "customer_address")
      ),
      (
        data.customerGroupDataSource,
        defaultOtherDataSourceConfig
          .copy(fieldName = "customer_group", groupByFieldNames = Seq("customer_id"))
      ),
      (
        data.groupDetailsDataSource,
        defaultOtherDataSourceConfig.copy(fieldName = "group_details")
      )
    )
    val dataRelationships = Seq(
      (data.customerIdDataRelationship, defaultDataRelationshipConfig),
      (data.groupIdDataRelationship, defaultDataRelationshipConfig)
    )

    val dataModel = DataModelGraph(
      rootDataSource = rootDataSource,
      rootDataSourceConfig = defaultRootDataSourceConfig,
      otherDataSources = otherDataSources,
      dataRelationships = dataRelationships,
      maxDepth = 14 // scalastyle:off magic.number
    )

    val nowSnapshotTime = SnapshotTime(0)

    val dataSnapshot = dataModel.readSnapshot(nowSnapshotTime)

    val df = dataSnapshot.read()

    df.printSchema()
    df.show()

    // TODO: store expected DataFrame output in Json/Parquet format under test resources,
    //  - read this, and check they are the same

    // TODO: create new tests for all failure conditions (and missingModes)
  }

  it should "instantiate correctly for ReallyLargeData" in {

    // TODO: add test for really large data model, (to test scaling)
    //  - > 100 DataSources
    //  - > 25 DataRelationships (with some exclusions?)

  }

  it should "fail to instantiate for XXX" in {

    // TODO: add tests for failure conditions:
    //  - two paths to same vertex
    //  - missing path to data source (when ERROR is set)
    //  - maxDepth exceeded
    //  - ...

  }

}
