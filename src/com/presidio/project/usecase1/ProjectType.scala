package com.presidio.project.usecase1

import com.presidio.project.utils.Utility
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ProjectType {


  def Product(): Unit = {
    val utility = new Utility
    utility.readFile("C:\\Users\\VC\\Downloads\\sales.csv")
    val ss = utility.getSparkSession
    val productLineDF = ss.sql("select `Product type` from SALES_TABLE where `Product line`='"+ProductLine.golf+"'")
    productLineDF.show
    val productLineRDD = productLineDF.rdd
    val productTypeRDD = productLineRDD.map(each => (each, 1))
    val productType = productTypeRDD.reduceByKey((x, y) => x + y)
    productType.foreach(println)
    val productCase=for{
      x<-productType
    }yield ProductTypes
    val productTypeCount = productCase.count()
    println("Number of Product Types: "+productTypeCount)
    ParquetFile(productType)
  }

  def ParquetFile(productType : RDD[(Row , Int)]): Unit ={
    val utility = new Utility
    val session = utility.getSparkSession
    val productTypeRow = productType.map(each => Row(each._1.toString()))

    val productSchema = StructType(Array(StructField("ProductTypes", StringType,true)))
    val productDataFrame = session.createDataFrame(productTypeRow,productSchema)
    productDataFrame.write.parquet("C:\\Users\\VC\\Downloads\\ProductLists")

    val outputFile  = session.read.parquet("C:\\Users\\VC\\Downloads\\ProductLists")
    println("Output")
    outputFile.show()

  }

}
case class ProductTypes(productType:Row,count:Int)
