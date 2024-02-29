package com.tutorial.spark.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def setupSparkSession(appName: String, masterUrl: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(masterUrl)
      .getOrCreate()
  }
}