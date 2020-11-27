package io.github.sparkdenormalize

import java.sql.Date

import io.github.sparkdenormalize.spark.LeftJoinListener
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}

// scalastyle:off magic.number
object SparkTests extends Logging {

  /** This is an example with 4 tables:
    *
    * 0. customer:
    *    - a fact table with `customer_id` for each customer
    * 1. customer_address:
    *    - a `customer_id` keyed dim table with customer address attributes
    *      (one-to-at-most-one relationship to customer)
    * 2. customer_group:
    *    - a translation from `customer_id` to `group_id` (one-to-many relationship to customer)
    * 3. group_details:
    *    - a `group_id` keyed dim table with group attributes (one-to-one relationship to group)
    *
    * We expect the output column structure to be:
    *  - data: STRUCT
    *     - fields from `customer`
    *  - customer_address: STRUCT
    *     - data: STRUCT
    *        - fields from `customer_address`
    *  - customer_group: ARRAY<STRUCT>
    *     - data: STRUCT
    *        - fields from `customer_group`
    *     - group_details: STRUCT
    *        - data: STRUCT
    *           - fields from `group_details`
    */
  def joinCustomerGroup()(implicit ss: SparkSession): Unit = {
    ss.listenerManager.register(new LeftJoinListener())

    // ----------------
    // customer
    // ----------------
    val customer = ss
      .createDataFrame(
        ss.sparkContext.parallelize(
          List(
            Row(1, "brad", new Date(1990, 1, 18), List("a", "b")),
            Row(2, "james", new Date(2005, 5, 6), List("a", "b")),
            Row(3, "hannah", new Date(1950, 8, 30), List("a", "b"))
          )
        ),
        StructType(
          List(
            StructField("customer_id", IntegerType, nullable = true),
            StructField("name", StringType, nullable = true),
            StructField("birth_date", DateType, nullable = true),
            StructField("test_list", ArrayType(StringType, true), nullable = true)
          )
        )
      )
      .select(col("*"), explode(col("test_list")).as("test_list_value"))

    val struct__customer = struct(
      customer.columns.map(customer(_)): _*
    )

    // TODO:
    //  - we should detect if people have done joins/etc in their VirtualDataSource using the SparkPlan
    //    (but this check should be in the DataSource class)
    val customer_WholeStageCodeGen: SparkPlan = customer.queryExecution.executedPlan
    val customer_attributeSet: AttributeSet = customer_WholeStageCodeGen.outputSet
    logWarning(s"CUSTOMER SCHEMA: ${customer_attributeSet}")

    // ----------------
    // customer_address
    // ----------------
    val customer_address = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row(1, "Australia", "VIC"),
          Row(2, "England", null), // scalastyle:ignore null
          Row(3, "USA", "GA"),
          Row(3, "NULL", "NULL") // TODO: this is a duplicate node, should throw error
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

    val struct__customer_address = struct(
      customer_address.columns.map(customer_address(_)): _*
    )

    // ----------------
    // customer_group
    // ----------------
    val customer_group = ss.createDataFrame(
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
          StructField("customer_id", IntegerType, true),
          StructField("group_id", StringType, true),
          StructField("relation_type", StringType, true)
        )
      )
    )

    val struct__customer_group = struct(
      customer_group.columns.map(customer_group(_)): _*
    )

    // ----------------
    // group_details
    // ----------------
    val group_details = ss.createDataFrame(
      ss.sparkContext.parallelize(
        List(
          Row("a", "Android", "device"),
          Row("b", "iPhone", "device"),
          Row("c", "iPad", "device")
        )
      ),
      StructType(
        List(
          StructField("group_id", StringType, true),
          StructField("name", StringType, true),
          StructField("type", StringType, true)
        )
      )
    )

    val struct__group_details = struct(
      struct(
        group_details.columns.map(group_details(_)): _*
      ).as("data")
    )

    // ----------------
    // JOIN - customer_group + group_details
    // ----------------
    val joined__part1 = customer_group.join(
      group_details,
      customer_group("group_id") === group_details("group_id"),
      "left"
    )

    // ----------------
    // GROUP BY - customer_group
    // ----------------
    // NOTE: use to reference join key
    val key__part1 = customer_group("customer_id")

    val grouped__part1 = joined__part1
      .groupBy(key__part1)
      .agg(
        collect_list(
          struct(
            struct__customer_group.as("data"),
            struct__group_details.as("group_details")
          )
        ).as("customer_group")
      )

    val struct_list__part1 = grouped__part1
      // NOTE: we drop the key column because it might be named the same as the `fieldName` of the table
      //       (which would make it impossible to reference the collected list column)
      .drop(key__part1)("customer_group")

    // ----------------
    // JOIN - customer + customer_address + grouped__part1
    // ----------------
    val joined__part2 = customer
      .join(
        customer_address,
        customer("customer_id") === customer_address("customer_id"),
        "left"
      )
      .join(
        grouped__part1,
        customer("customer_id") === key__part1,
        "left"
      )

    // ----------------
    // OUTPUT
    // ----------------
    val output = joined__part2.select(
      // customer
      struct__customer.as(
        "data"
      ),
      // customer_address
      struct(struct__customer_address.as("data")).as(
        "customer_address"
      ),
      // customer_group + group_details
      struct_list__part1.as(
        "customer_group"
      )
    )
    output.show()
    output.printSchema()

  }

  def joinRealData()(implicit ss: SparkSession): Unit = {

    ss.listenerManager.register(new LeftJoinListener())

    // table - customer
    val customer = ss.read
      .parquet(
        "gs://reece--rdh-prod--raw--trs-au/data/customer.parquet/snapshot_epoch=1602493162"
      )
      .sample(0.1)

    // struct - customer
    val struct__customer = struct(
      customer.columns.map(customer(_)): _*
    ).as("customer")

    // table - customer_contact
    val customer_contact = ss.read
      .parquet(
        "gs://reece--rdh-prod--raw--trs-au/data/customer_contact.parquet/snapshot_epoch=1602493162"
      )

    // struct - customer_contact
    val struct__customer_contact = struct(
      customer_contact.columns.map(customer_contact(_)): _*
    ).as("customer_contact")

    // join
    val joined = customer.join(
      customer_contact,
      customer("cmcustno") === customer_contact("cc_customer_number"),
      "left"
    )

    // create output
    val output = joined.select(
      struct__customer,
      struct__customer_contact
    )

    output.show()
    output.printSchema()
  }

}
