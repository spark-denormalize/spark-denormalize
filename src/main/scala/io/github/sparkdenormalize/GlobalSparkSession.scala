package io.github.sparkdenormalize

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** the trait which provides the global implicit SparkSession to Main */
trait GlobalSparkSession {

  /** the global SparkSession */
  implicit val ss: SparkSession = {
    val existingSession: Option[SparkSession] =
      SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (existingSession.isDefined && !existingSession.get.sparkContext.isStopped) {
      existingSession.get
    } else {
      val conf = new SparkConf()

      // set default configs
      conf.setIfMissing("spark.master", "local[4]")
      conf.setIfMissing("spark.app.name", "spark-denormalize")

      // restrict dangerous configs
      require(
        !conf.getBoolean("spark.sql.caseSensitive", defaultValue = false),
        "`spark.sql.caseSensitive` must be `false`"
      )

      SparkSession.builder
        .config(conf)
        .getOrCreate()
    }
  }

}
