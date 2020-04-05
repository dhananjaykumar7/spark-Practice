//This snippet creates a pair RDD by splitting by space on every element in an RDD,
// flatten it to form a single word string on each element in RDD
// and finally assigns an integer “1” to every word.
package com.test.com.customer
import org.apache.spark.sql.SparkSession

object PairRddExample {
  def main(args : Array[String]) :Unit={
    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
    val wordsRdd = rdd.flatMap(_.split(" "))
    //val wordsRdd = rdd.flatMap(line =>line.split(" "))

    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)
    //distinct – Returns distinct keys.
    //pairRDD.distinct.foreach(println)
    pairRDD.sortByKey().foreach(println)

    println("******************************")
    println("reduceByKey – Transformation returns an RDD after adding value for each key")
    val wordcount = pairRDD.reduceByKey((a,b)=>a+b)
    wordcount.foreach(println)
    spark.stop()
  }

}
