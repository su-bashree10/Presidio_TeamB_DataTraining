package com.presidio.project.usecase1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait ProjectTypeSales {

  def Product(): Unit ;
  def ParquetFile(productType : RDD[(Row , Int)]): Unit;
}
