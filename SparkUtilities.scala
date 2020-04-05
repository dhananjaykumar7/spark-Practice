package com.test.com.customer

import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import scala.xml.XML;

object SparkUtilities {
  def getSparkSession(appName: String) : SparkSession ={
    val spark= SparkSession.builder.appName("TagQuesScore").master("local").getOrCreate()
   //val spark = SparkSession.builder.appName("TagQuesScore").master("local").getOrCreate()
    spark
  }

  def getSparkContext(appName:String):SparkContext={
    val conf=new SparkConf().setAppName(appName).setMaster("local")
    val sc=new SparkContext(conf)
    sc
  }

  def convertCurrencyToDouble(currency:String):Double={
    currency.stripPrefix("$").trim.toDouble
  }
  def getDate(date:String):Timestamp={
    new java.sql.Timestamp(CIConstants.formatter.parseDateTime(date).getMillis)
  }
}
