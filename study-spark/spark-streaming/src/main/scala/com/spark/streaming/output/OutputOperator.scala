package com.spark.streaming.output

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @fileName: OutputOperator.java
  * @description: 输出函数
  * @author: by echo huang
  * @date: 2020-07-05 00:31
  */
object OutputOperator {
  def main(args: Array[String]): Unit = {
    //默认是一个采集周期是一个RDD，窗口函数就存在多个RDD，最终形成一个DStream
    val context = new StreamingContext("local[4]", "foreachRDD", Seconds(3))

    val socketStream: ReceiverInputDStream[String] = context.socketTextStream("localhost", 9999)


    socketStream.foreachRDD(rdd => {
      rdd.map("Hello:" + _)
        .collect()
        .foreach(println)
    })

    context.start()
    context.awaitTermination()
  }
}
