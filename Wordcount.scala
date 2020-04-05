/*
package com.test

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object Wordcount {
  def main(args: Array[String]) : Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    /*if (args.length < 2) {
      System.err.println("Usage: WordCount <hostname> <port>")
      System.exit(1)
    }*/
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Wordcount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //                                            10 second batch size

    //val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = ssc.socketTextStream("127.0.0.1",4444)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
//Create a socket stream on target ip:port
//nc -lk 9099

//bin/spark-submit --class com.df.spark.stream.wc.Wordcount ../streamJob.jar localhost 9099
*/
