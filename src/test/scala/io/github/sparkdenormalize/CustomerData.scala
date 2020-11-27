package io.github.sparkdenormalize

import java.sql.Date

import io.github.sparkdenormalize.core.datasource.{DataSource, DummyDataSource}
import io.github.sparkdenormalize.core.relationship.DataRelationship
import io.github.sparkdenormalize.core.relationship.DataRelationship.JoinKeyComponent
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._

// scalastyle:off magic.number
class CustomerData()(implicit ss: SparkSession) {

  // ----------------
  // DataSources
  // ----------------
  val customerDataSource: DataSource = DummyDataSource(
    name = "customer",
    dataFrame = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row(1, "brad", new Date(1990, 1, 18)),
          Row(2, "james", new Date(2005, 5, 6)),
          Row(3, "hannah", new Date(1950, 8, 30))
        )
      ),
      StructType(
        List(
          StructField("customer_id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("birth_date", DateType, nullable = true)
        )
      )
    )
  )

  val customerAddressDataSource: DataSource = DummyDataSource(
    name = "customer_address",
    dataFrame = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row(1, "Australia", "VIC"),
          Row(2, "England", null), // scalastyle:ignore null
          Row(3, "USA", "GA")
        )
      ),
      StructType(
        List(
          StructField("customer_id", IntegerType, nullable = true),
          StructField("country", StringType, nullable = true),
          StructField("state", StringType, nullable = true)
        )
      )
    )
  )

  val customerGroupDataSource: DataSource = DummyDataSource(
    name = "customer_group",
    dataFrame = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row(1, "b", "primary"),
          Row(1, "c", "secondary"),
          Row(2, "a", "primary"),
          Row(3, "c", "primary")
        )
      ),
      StructType(
        List(
          StructField("customer_id", IntegerType, nullable = true),
          StructField("group_id", StringType, nullable = true),
          StructField("relation_type", StringType, nullable = true)
        )
      )
    )
  )

  val groupDetailsDataSource: DataSource = DummyDataSource(
    name = "group_details",
    dataFrame = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row("a", "Android", "device"),
          Row("b", "iPhone", "device"),
          Row("c", "iPad", "device")
        )
      ),
      StructType(
        List(
          StructField("group_id", StringType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("type", StringType, nullable = true)
        )
      )
    )
  )

  // ----------------
  // DataRelationships
  // ----------------
  private val customerJoinKeyComponents = Seq(
    JoinKeyComponent(
      fieldsMap = Map("customer_id" -> IntegerType),
      query = expr("ROUND(customer_id, 0)"),
      castDataType = StringType
    )
  )

  private val groupJoinKeyComponents = Seq(
    JoinKeyComponent(
      fieldsMap = Map("group_id" -> StringType),
      query = expr("group_id"),
      castDataType = StringType
    )
  )

  val customerIdDataRelationship: DataRelationship = DataRelationship(
    name = "customer_id",
    dataSourceJoinKeyMap = Map(
      customerDataSource -> customerJoinKeyComponents,
      customerAddressDataSource -> customerJoinKeyComponents,
      customerGroupDataSource -> customerJoinKeyComponents
    )
  )

  val groupIdDataRelationship: DataRelationship = DataRelationship(
    name = "group_id",
    dataSourceJoinKeyMap = Map(
      customerGroupDataSource -> groupJoinKeyComponents,
      groupDetailsDataSource -> groupJoinKeyComponents
    )
  )
}
