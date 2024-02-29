package com.tutorial.spark.analysis

import com.tutorial.spark.model.{MemberVolume, Trade, TradeVolume}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, SparkSession}

class TradeAnalysis(spark: SparkSession) {

  import spark.implicits._

  def runAnalysis(): Unit = {
    val tradesDS = createSampleData()
    val topTradedStocksDS = calculateTopTradedStocks(tradesDS)
    val totalVolumeByMemberDS = calculateTotalVolumeByMember(tradesDS)
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
    println("Top traded stocks:")
    topTradedStocksDS.show()

    println("Total trading volume by member:")
    totalVolumeByMemberDS.show()
  }
}