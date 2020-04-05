package com.test

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object AvgAnsTime {
   def main(args : Array[String])={
     System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
     System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")
     val spark = SparkSession
       .builder
       .appName("AvgAnsTime")
       .master("local")
       .getOrCreate()

     val data = spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd

     val rs= data.filter(line => {line.trim().startsWith("<row")})
       .filter(line => {line.contains("PostTypeId=\"1\"")})
     /*val result = data.filter{line => {line.trim().startWith("<row")}}
       .filter{line => {line.contains("PostTypeId=\"1\"")}}
*/
     rs.foreach{ println }
     println("total count: " + rs.count())

     spark.stop


   }

}
