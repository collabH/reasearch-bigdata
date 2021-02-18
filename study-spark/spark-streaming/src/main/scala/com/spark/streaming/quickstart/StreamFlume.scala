package com.spark.streaming.quickstart

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * @fileName: StreamFlume.scala
 * @description: StreamFlume.scala类说明
 * @author: by echo huang
 * @date: 2021/2/18 9:50 上午
 */
object StreamFlume {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val context = new StreamingContext(conf, Durations.seconds(5))

    // 接受flume avro数据
    val flumeStream = FlumeUtils.createStream(context, "hadoop001", 8888)

    flumeStream.print()
    context.start()
    context.awaitTermination()
  }
}
