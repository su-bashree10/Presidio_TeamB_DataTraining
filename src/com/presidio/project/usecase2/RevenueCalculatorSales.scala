package com.presidio.project.usecase2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

trait RevenueCalculatorSales {

  def calculateRevenue(): Unit ;
  def parquetFile(totalRevenue : DataFrame): Unit;
}
