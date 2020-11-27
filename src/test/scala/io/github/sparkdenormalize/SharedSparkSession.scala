package io.github.sparkdenormalize

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

/** Provides a shared SparkSession for all tests in a suite */
trait SharedSparkSession extends BeforeAndAfterAll with Logging {
  self: UnitSpec =>

  @transient private var _ss: Option[SparkSession] = None

  implicit final def ss: SparkSession = {
    if (_ss.isEmpty) {
      _ss = Some(createSparkSession())
    }
    _ss.get
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (_ss.isDefined) {
      ss.stop()
      _ss = None

      // avoid rebinding to same port (RPC unbind takes time after SparkSession.stop())
      System.clearProperty("spark.driver.port")
    }
  }

  private def createSparkSession(): SparkSession = {
    // ensure there is no existing SparkSession in the JVM
    val existingSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (existingSession.isDefined) {
      assert(
        existingSession.get.sparkContext.isStopped,
        "SharedSparkSession() trait requires there is not already a running SparkSession in the JVM"
      )
    }

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("spark-testing")
      .set("spark.ui.enabled", "false")

    logWarning(s"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logWarning(s"SparkSession for: ${this.getClass.getSimpleName}")
    logWarning(s"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    SparkSession.builder
      .config(conf)
      .getOrCreate()
  }

}
