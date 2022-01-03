package com.presidio.project.usecase2

import com.presidio.project.utils.Utility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class RevenueCalculator extends RevenueCalculatorSales {

  def calculateRevenue(): Unit = {
    val utility = new Utility
    utility.readFile("sales.csv")
    val ss = utility.getSparkSession
    val totalRevenue = ss.sql("select `Revenue` from SALES_TABLE where `Retailer country`='France'")
    val totalDF = totalRevenue.agg(sum("Revenue").cast("long")).withColumnRenamed("CAST(sum(Revenue) AS BIGINT)","totalRevenue")
    val total = totalDF.first.getLong(0)
    println(total)
    parquetFile(totalDF)
  }

  def parquetFile(totalRevenue: DataFrame): Unit = {
    val utility = new Utility
    val session = utility.getSparkSession
    totalRevenue.write.parquet("Revenue")
    val outputFile  = session.read.parquet("Revenue")
    println("Output")
    outputFile.show()
  }
}
