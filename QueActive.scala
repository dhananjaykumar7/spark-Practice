package com.test

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.xml.XML


object QueActive {

  def main(args: Array[String]): Unit = {

  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
  System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession.builder().appName("QueActive").master("local").getOrCreate();

    val data=spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd

    val result = data.filter{line => {line.trim().startsWith("<row")}}.
      filter{line => {line.contains("PostTypeId=\"1\"")}}
      .flatMap{line => {
        val xml = XML.loadString(line)
        xml.attribute("CreationDate")

      }
      }.map{line => {
      (format2.format(format.parse(line.toString())).toString(), 1)
    }}.reduceByKey(_+_)
    result.foreach { println }

    spark.stop
  }
}
