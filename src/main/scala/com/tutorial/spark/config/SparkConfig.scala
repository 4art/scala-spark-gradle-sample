package com.tutorial.spark.config

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkConfig {
  def setupSparkSession(appName: String, url: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(url)
      .getOrCreate()

    // Programmatically set log level
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR) // Reduce Spark verbosity

    spark
  }
}