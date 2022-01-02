package main.java.com.presidio.project

import main.java.com.presidio.project.utils.Utility

object Sales extends Utility{

  def main(args: Array[String]): Unit = {
    val csvDF = readCSV("/Users/VC/Downloads/sales.csv")
    csvDF.show(10)
  }
}
