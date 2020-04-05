package com.test.com.customer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object IOUtilities {

  def readProductsDF(spark:SparkSession,path:String):DataFrame={
    import spark.implicits._
    spark.read.textFile(path)
      .map(line => line.split(CIConstants.DELIMITER))
      .map(fields => new Product2(fields(0).toInt,fields(1),fields(2), fields(3),SparkUtilities.convertCurrencyToDouble(fields(4)))).toDF()
  }

  def readSalesDF(spark:SparkSession,path:String):DataFrame={
    import spark.implicits._
    spark.read.textFile(path)
      .map(line =>line.split(CIConstants.DELIMITER))
      .map(fields => new Sales2(fields(0).toInt,fields(1).toInt,fields(2).toInt, SparkUtilities.getDate(fields(3)), SparkUtilities.convertCurrencyToDouble(fields(4)),fields(5).toInt))
      .toDF()
  }
  def readRefundDF(spark:SparkSession,path:String)={
    import spark.implicits._
    spark.read.textFile(path)
      .map(line => line.split(CIConstants.DELIMITER))
      .map(fields => new Refund2(fields(0).toInt,fields(1).toInt,fields(2).toInt,fields(3).toInt, SparkUtilities.getDate(fields(4)), SparkUtilities.convertCurrencyToDouble(fields(5)),fields(6).toInt))
      .toDF()
  }
  def readCustomerDF(spark:SparkSession,path:String)= {
    import spark.implicits._
    spark.read.textFile(path)
      .map( line => line.split(CIConstants.DELIMITER))
      .map(fields => new Customer2(fields(0).toInt, fields(1), fields(2), fields(3).toLong))
      .toDF()
  }
  def writeDF(df:DataFrame,path:String):Unit={
    df.repartition(1)
      .write
      .format("csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save(path)

  }
}
