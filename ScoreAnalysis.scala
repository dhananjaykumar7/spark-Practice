package com.test
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object ScoreAnalysis {

  def main(args : Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    val spark = SparkSession.builder.appName("ScoreAnalysis").master("local").getOrCreate();

    val data = spark.read.textFile("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\code\\Posts.xml").rdd

  val rs = data.filter(line => {line.trim().startsWith("<row")}).
    filter(line => {line.contains("PostTypeId=\"1\"")}).
    map(line => {
      val xml = XML.loadString(line)
      (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()),line)
    }).sortByKey(false)

rs.take(10).foreach(println)
    spark.stop
  }

  }
