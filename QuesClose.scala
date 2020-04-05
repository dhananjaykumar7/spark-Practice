package com.test

import org.apache.spark.sql.SparkSession

import scala.xml.XML

object QuesClose {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val format2 = new SimpleDateFormat("yyyy-MM")
    val spark = SparkSession.builder.appName("QueAns0").master("local").getOrCreate();

    val data = spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd

    val rs= data.filter( line => {line.trim.startsWith("<row")})
      .filter(line => {line.contains("PostTypeId=\"1\"")})
      .map(line => {
        val xml=XML.loadString(line)
        var closeDate = ""
        if(xml.attribute("ClosedDate") !=None){
          val clDate = xml.attribute("ClosedDate").get.toString()
          closeDate = format2.format(format.parse(clDate))
        }
        (closeDate, 1)
      }).filter(line => {line._1.length()>0}).reduceByKey(_+_)

    rs.foreach(println)

    println(rs.count())
    spark.stop()
  }

}
