package com.spark.streaming.quickstart


import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @fileName: WordCount.java
  * @description: WordCount.java类说明
  * @author: by echo huang
  * @date: 2020-07-01 20:41
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local[4]").appName("SparkTestProject").getOrCreate

    //采集周期,以这个时间为周期来采集实时的数据
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(3))

    //从指定的端口采集数据
    val socketDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
    val wordDStream: DStream[String] = socketDStream.flatMap(_.split(","))
    val tupleDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordCount: DStream[(String, Int)] = tupleDStream.reduceByKey(_ + _)

    wordCount.print()

    //启动
    streamingContext.start()
    //等待采集器的执行
    streamingContext.awaitTermination()
  }

}
