package com.test.com.customer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

object UIDStats {
  val conf = new SparkConf().setAppName("Aadhaar dataset analysis using Spark").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args :Array[String]):Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")
    val aadharData=sc.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\data\\UIDAI-ENR-DETAIL-20170308.csv")
    val header= aadharData.first()
    val aadharDataWithoutHeader = aadharData.filter(rec => rec != header)
    aadharData.take(5).foreach(println)
    aadharDataWithoutHeader.take(5).foreach(println)

  }
}
