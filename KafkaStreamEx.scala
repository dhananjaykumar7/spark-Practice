/*
package com.test

import org.apache.spark.SparkConf
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


class KafkaStreamEx {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("Local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(20))

    val kafkaconf = Map(
      "Zookeeper.connect" -> "localhost:2181",
      "group.id" -> "test-consumer-group",
      "zookeeper.connection.timout.ms" ->"5000"
    )

    val lines = KafkaUtils.createStream[Atrray[Byte],String,DefaultDecoder,StringDecoder](ssc,kafkaconf,
      Map("test"->1),StoregeLevel.Memory_ONLY)
    val words = lines.flatMap( Case(x,y) => y.s)

  }

}
*/
