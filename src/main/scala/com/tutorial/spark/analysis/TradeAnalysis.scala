package com.tutorial.spark.analysis

import com.tutorial.spark.model.{MemberVolume, Trade, TradeVolume}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, SparkSession}

class TradeAnalysis(spark: SparkSession) {
  private val logger = Logger.getLogger(getClass.getName)

  import spark.implicits._

  def runAnalysis(): Unit = {
    logger.info("Creating sample data")
    val tradesDS = createSampleData()

    logger.info("Calculating top traded stocks")
    val topTradedStocksDS = calculateTopTradedStocks(tradesDS)

    logger.info("Calculating total volume by member")
    val totalVolumeByMemberDS = calculateTotalVolumeByMember(tradesDS)

    logger.info("Showing results")
    showResults(topTradedStocksDS, totalVolumeByMemberDS)
  }

  private def createSampleData(): Dataset[Trade] = {
    Seq(
      Trade("Member1", "AAPL", 50, 120.50),
      Trade("Member2", "GOOGL", 30, 1520.10),
      Trade("Member1", "MSFT", 20, 210.40),
      Trade("Member3", "AAPL", 15, 121.00),
      Trade("Member2", "AAPL", 5, 119.50)
    ).toDS()
  }

  private def calculateTopTradedStocks(tradesDS: Dataset[Trade]): Dataset[TradeVolume] = {
    tradesDS.groupBy(tradesDS("stock"))
      .agg(sum(tradesDS("quantity")).alias("totalQuantity"))
      .as[TradeVolume]
  }

  private def calculateTotalVolumeByMember(tradesDS: Dataset[Trade]): Dataset[MemberVolume] = {
    tradesDS.groupBy(tradesDS("member"))
      .agg(sum(tradesDS("quantity") * tradesDS("price")).alias("totalVolume"))
      .as[MemberVolume]
  }

  private def showResults(topTradedStocksDS: Dataset[TradeVolume], totalVolumeByMemberDS: Dataset[MemberVolume]): Unit = {
    logger.info("Top traded stocks:")
    topTradedStocksDS.show()

    logger.info("Total trading volume by member:")
    totalVolumeByMemberDS.show()
  }
}