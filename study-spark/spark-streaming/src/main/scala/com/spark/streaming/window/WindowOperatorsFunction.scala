package com.spark.streaming.window

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * @fileName: WindowOperatorsFunction.java
  * @description: WindowOperatorsFunction.java类说明
  * @author: by echo huang
  * @date: 2020-07-04 00:09
  */
object WindowOperatorsFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("window")
      .setMaster("local[4]")
    val params: Map[String, Object] = Map(KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,localhost:9093,localhost:9093",
      GROUP_ID_CONFIG -> "spark-group-id",
      CLIENT_ID_CONFIG -> "spark-client-id",
      AUTO_OFFSET_RESET_CONFIG -> "latest",
      ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val context = new StreamingContext(conf, Seconds(3))
    context.checkpoint("checkpoint")
    val kafkaStream: DStream[String] = KafkaUtils.createDirectStream(context, locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](List("stateful-topic"), params)).map(record => {
      record.value()
    })
    kafkaStream.window(windowDuration = Durations.seconds(10), Durations.seconds(10))
 }

}
