package com.tutorial.spark

import com.tutorial.spark.config.SparkConfig
import com.tutorial.spark.analysis.TradeAnalysis

object TradeAnalysisApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.setupSparkSession("TradeAnalysis", "local[*]")
    try {
      new TradeAnalysis(spark).runAnalysis()
    } finally {
      spark.stop()
    }
  }
}

