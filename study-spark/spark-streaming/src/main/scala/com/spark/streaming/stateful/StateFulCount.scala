package com.spark.streaming.stateful

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @fileName: StateFulCount.java
  * @description: StateFulCount.java类说明
  * @author: by echo huang
  * @date: 2020-07-02 13:42
  */
object StateFulCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("stateful")
      .getOrCreate()
    val params: Map[String, Object] = Map(KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,localhost:9093,localhost:9093",
      GROUP_ID_CONFIG -> "spark-group-id",
      CLIENT_ID_CONFIG -> "spark-client-id",
      AUTO_OFFSET_RESET_CONFIG -> "latest",
      ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val context = new StreamingContext(spark.sparkContext, Seconds(3))
    context.checkpoint("checkpoint")
    val kafkaStream: DStream[String] = KafkaUtils.createDirectStream(context, locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](List("stateful-topic"), params)).map(record => {
      record.value()
    })
    kafkaStream.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .updateStateByKey(updateFunction)
      .print()


    //启动
    context.start()
    context.awaitTermination()

  }

  /**
    * update state function
    *
    * @param newValues    新值数据
    * @param runningCount 运行次数
    * @return
    */
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val count: Int = runningCount.getOrElse(0) + newValues.sum
    Option(count)
  }

}
