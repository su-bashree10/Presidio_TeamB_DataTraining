package com.presidio.project.usecase1

object ProjectMain {


  def main(args: Array[String]): Unit = {
    // Invoke Product method in ProductType  
    val productType = new ProjectType
    productType.Product()
  }

}
