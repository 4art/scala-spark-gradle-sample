package com.tutorial.spark.config

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkConfig {
  def setupSparkSession(appName: String, url: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(url)
      .getOrCreate()

    // Configure log4j
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    spark
  }
}