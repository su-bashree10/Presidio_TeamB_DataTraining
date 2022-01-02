package main.java.com.presidio.project.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Utility {
  private def getSparkSession: SparkSession = {
    System.setProperty("hadoop.home.dir", "D:\\data\\winutils")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataSalesProj")
      .getOrCreate()
    spark
  }
  def readCSV(path: String): DataFrame = {
    val spark = getSparkSession
    val df = spark.read.format("csv").option("header", "true").csv(path)
    df
  }
}
