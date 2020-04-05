package com.test

import org.apache.spark.sql.SparkSession

import scala.xml.XML
import java.text.SimpleDateFormat
object ActiveQuesMore6 {
  def main(args : Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val spark = SparkSession.builder.appName("QueTitle").master("local").getOrCreate()
    val data = spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd
    val rs= data.filter( line => {line.trim.startsWith("<row")})
      .filter(line => {line.contains("PostTypeId=\"1\"")})
      .map(line => {
        val xml=XML.loadString(line)
        (xml.attribute("CreationDate").get,xml.attribute("LastActivityDate").get,line)
      }).map(data => {
      val crDate =format.parse(data._1.text)
      val crTime = crDate.getTime

      val laDate= format.parse(data._2.text)
      val laTime=  laDate.getTime

      val timeDeff =   laTime - crTime
      (crDate,laDate,timeDeff,data._3)


    }).filter(data => {data._3/(1000*60*60*24)>30*6})

    rs.foreach { println }
    println(rs.count())
    //			println(result.count())

    spark.stop
  }

  }
