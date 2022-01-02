package com.presidio.project.usecase1

import com.presidio.project.utils.Utility

class ProjectType {


  def Product(): Unit = {
    val utility = new Utility
    utility.readCSV("C:\\Users\\VC\\Downloads\\sales.csv")
    val ss = utility.getSparkSession
    val productLineDF = ss.sql("select `Product type` from PRODUCT_TABLE where `Product line`='Golf Equipment'")
    productLineDF.show
    val productLineRDD = productLineDF.rdd
    val productTypeRDD = productLineRDD.map(each => (each, 1))
    val productType = productTypeRDD.reduceByKey((x, y) => x + y)
    productType.foreach(println)
    val productTypeCount = productType.count()

  }

}
