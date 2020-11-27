package io.github.sparkdenormalize

import io.github.sparkdenormalize.config.ConfigManager
import io.github.sparkdenormalize.core.output.OutputDataset
import org.apache.spark.internal.Logging

object Main extends GlobalSparkSession with Logging {

  def main(args: Array[String]) {
    // set spark LogLevel to WARN
    logInfo(f"================")
    logInfo(f"Setting Spark LogLevel: WARN")
    logInfo(f"================")
    ss.sparkContext.setLogLevel("WARN")

    val conf = new ArgumentParser(args)
    logInfo(f"================")
    logInfo(f"PROGRAM ARGUMENTS:")
    logInfo(f"================")
    logInfo(f"--config = ${conf.config.apply()}")
    logInfo(f"================")

    // initialize ConfigManager
    val configFilePaths: Seq[String] = conf.config.apply()
    val configManager = ConfigManager(configFilePaths)

    // sync all OutputDatasets
    val outputDatasets: Seq[OutputDataset] = configManager.allOutputDataset()
//    outputDatasets.foreach(_.sync())

    // TODO: remove after debug
//    SparkTests.joinCustomerGroup()
//    SparkTests.joinRealData()
//    Thread.sleep(1000 * 60 * 10)

    // stop the spark session
    ss.stop()
  }

}
