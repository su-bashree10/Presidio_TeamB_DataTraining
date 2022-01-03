package com.presidio.project

import com.presidio.project.usecase1.ProjectType
import com.presidio.project.usecase2.RevenueCalculator

object ProjectMain {


  def main(args: Array[String]): Unit = {
    // Invoke Product method in ProductType
    val productType = new ProjectType
    productType.Product()
    val revenueCalculator = new RevenueCalculator
    revenueCalculator.calculateRevenue()
  }

}
