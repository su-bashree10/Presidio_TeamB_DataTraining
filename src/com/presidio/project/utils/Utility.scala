package com.presidio.project.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utility {
    
    // to create and return spark session
    def getSparkSession: SparkSession = {
      System.setProperty("hadoop.home.dir", "D:\\winutils")
      val spark = SparkSession.builder
        .master("spark://aliasgar-akc:7077")
        .appName("DataSalesProj")
        .getOrCreate()
      spark
    }
    
    // to read the csv file
    def readFile(path: String): Unit = {
      val sparkContext = getSparkSession
      val csvDF = sparkContext.read
        .option("header", "true")
        .csv(path)
      csvDF.createOrReplaceTempView("SALES_TABLE")
    }
}
