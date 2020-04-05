package com.test
import scala.xml.XML

import org.apache.spark.sql.SparkSession
import scala.xml.Elem

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object QueTitle {

  def main(args : Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

 val spark = SparkSession.builder.appName("QueTitle").master("local").getOrCreate()
    val data = spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd

    val result = data.filter{line =>{line.trim().startsWith("<row")}}
      .filter( line => {line.contains("PostTypeId=\"1\"")})
      .flatMap(line => {
        val xml = XML.loadString(line)
        xml.attribute("Title")
      }
      ).filter(line => {line.mkString.toLowerCase().contains("hadoop")})

    result.foreach{ println }
  println ("Result Count: " + result.count())

  spark.stop

  }

}
