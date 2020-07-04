package com.spark.streaming.quickstart

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @fileName: ReadKafkaStream.java
  * @description: ReadKafkaStream.java类说明
  * @author: by echo huang
  * @date: 2020-07-01 23:33
  */
object ReadKafkaStream {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("kafkaReader")
      .getOrCreate()
    val context = new StreamingContext(session.sparkContext, Seconds(3))

    val params: Map[String, Object] = Map(KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,localhost:9093,localhost:9093",
      GROUP_ID_CONFIG -> "spark-group-id",
      CLIENT_ID_CONFIG -> "spark-client-id",
      AUTO_OFFSET_RESET_CONFIG -> "latest",
      ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val kafkaDStream: DStream[String] = KafkaUtils.createDirectStream(context, locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](List("spark-topic"), kafkaParams = params)).map(value => value.value())
    kafkaDStream.print()

    context.start()
    context.awaitTermination()
  }

}
