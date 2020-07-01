package com.spark.streaming.quickstart

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @fileName: CreateDStream.java
  * @description: CreateDStream.java类说明
  * @author: by echo huang
  * @date: 2020-07-01 22:18
  */
object CreateDStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("createDs")
    val context = new StreamingContext(conf, Seconds(3))
    //    textFileStream(context)
    //    rddDStream(context)
    customDataSource(context)
    //启动
    context.start()
    context.awaitTermination()
  }

  def textFileStream(streamingContext: StreamingContext) = {
    val fileDStream: DStream[String] = streamingContext.textFileStream("file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/logs")
    fileDStream.print()
  }

  def rddDStream(streamingContext: StreamingContext) = {
    val rdd: RDD[String] = streamingContext.sparkContext.makeRDD(List("hello", "wy"))
    val queue = new mutable.Queue[RDD[String]]()
    for (i <- 1 to 10) {
      queue.enqueue(rdd)
    }
    streamingContext.queueStream(queue).print()
  }

  def customDataSource(streamingContext: StreamingContext) = {
    val receiver = new MyReceiver("localhost", 9999)
    val value: ReceiverInputDStream[String] = streamingContext.receiverStream(receiver)
    value.start()
    value.print()
  }
}


/**
  * 1.继承Receiver,传递RDD存储级别
  * 2.重写相关方法
  */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //_占位符，懒加载
  private var socket: Socket = _


  def receive(): Unit = {
    socket = new Socket(host, port)

    //拿到网络中的数据流
    val inputStream: InputStream = socket.getInputStream

    val reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8.name()))
    var line: String = null

    while ((line = reader.readLine()) != null) {
      if ("end".equals(line)) {
        return
      }
      //将采集的数据存储到采集器的内部进行转换
      this.store(line)
    }

  }

  /**
    * 启动Receiver
    */
  override def onStart(): Unit = {
    new Thread(() => {
      receive()
    })
  }

  /**
    * 停止Receiver
    */
  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
