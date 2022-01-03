package com.presidio.project.usecase2

import com.presidio.project.utils.Utility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class RevenueCalculator {
  def calculateRevenue(): Unit = {
    val utility = new Utility
    utility.readFile("C:\\Users\\VC\\Downloads\\sales.csv")
    val ss = utility.getSparkSession
    val totalRevenue = ss.sql("select `Revenue` from SALES_TABLE where `Retailer country`='France'")
    val totalDF = totalRevenue.agg(sum("Revenue").cast("long")).withColumnRenamed("CAST(sum(Revenue) AS BIGINT)","totalRevenue")
    val total = totalDF.first.getLong(0)
    parquetFile(totalDF)
  }

  def parquetFile(totalRevenue:DataFrame): Unit ={
    val utility = new Utility
    val session = utility.getSparkSession
    totalRevenue.write.parquet("C:\\Users\\VC\\Downloads\\Revenue")
    val outputFile  = session.read.parquet("C:\\Users\\VC\\Downloads\\Revenue")
    println("Output")
    outputFile.show()

  }
}
