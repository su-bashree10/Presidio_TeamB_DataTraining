package com.presidio.project.usecase1

import com.presidio.project.utils.Utility
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ProjectType extends ProjectTypeSales {

  // Count the number of product types for "Golf Equipment" 
  def Product(): Unit = {

    // Reading CSV file
    val utility = new Utility
    utility.readFile("sales.csv")

    // Getting Spark Session
    val ss = utility.getSparkSession

    // Fetching Product Type column
    val productLineDF = ss.sql("select `Product type` from SALES_TABLE where `Product line`='"+ProductLine.golf+"'")
    productLineDF.show

    // Converting DF to RDD format
    val productLineRDD = productLineDF.rdd

    // Mapping RDD
    val productTypeRDD = productLineRDD.map(each => (each, 1))

    // Grouping by similar values
    val productType = productTypeRDD.reduceByKey((x, y) => x + y)
    productType.foreach(println)

    // Using case class for storing Product Type
    val productCase=for{
      x<-productType
    }yield ProductTypes

    // Counting number of product types for "Golf Equipment"
    val productTypeCount = productCase.count()
    println("Number of Product Types: "+productTypeCount)

    // Storing as ParquetFile
    ParquetFile(productType)
  }

  def ParquetFile(productType : RDD[(Row , Int)]): Unit ={
    // Getting Spark Session
    val utility = new Utility
    val session = utility.getSparkSession

    // Converting RDD[(Row , Int)] to RDD[(Row)]
    val productTypeRow = productType.map(each => Row(each._1.toString()))
    val productSchema = StructType(Array(StructField("ProductTypes", StringType,true)))

    // Converting RDD[(Row)] to DataFrame
    val productDataFrame = session.createDataFrame(productTypeRow,productSchema)

    // Writing into ParquetFile
    productDataFrame.write.parquet("ProductLists")

    // Reading from ParquetFile
    val outputFile  = session.read.parquet("ProductLists")
    println("Output")
    outputFile.show() }

}
case class ProductTypes(productType:Row,count:Int)
