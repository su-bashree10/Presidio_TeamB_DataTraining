package com.presidio.project.usecase2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait RevenueCalculatorSales {

  def calculateRevenue(): Unit ;
  def ParquetFile(productType : RDD[(Row , Int)]): Unit;
}
