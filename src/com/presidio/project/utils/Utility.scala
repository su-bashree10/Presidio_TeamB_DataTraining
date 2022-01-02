package com.presidio.project.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utility {
    def getSparkSession: SparkSession = {
      System.setProperty("hadoop.home.dir", "D:\\winutils")
      val spark = SparkSession.builder
        .master("local[*]")
        .appName("DataSalesProj")
        .getOrCreate()
      spark
    }
    def readCSV(path: String): Unit = {
      val sparkContext = getSparkSession
      val csvDF = sparkContext.read
        .option("header", "true")
        .csv(path)
      csvDF.createOrReplaceTempView("PRODUCT_TABLE")
    }
}