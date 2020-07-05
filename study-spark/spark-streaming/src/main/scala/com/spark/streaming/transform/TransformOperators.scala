package com.spark.streaming.transform

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @fileName: TransformOperators.java
  * @description: 转换算子
  * @author: by echo huang
  * @date: 2020-07-05 00:15
  */
object TransformOperators {

  def main(args: Array[String]): Unit = {

    val context = new StreamingContext("local[4]", "transform", Seconds(3))
    val socketStream: ReceiverInputDStream[String] = context.socketTextStream("localhost", 9999)


 /*   //TODO 代码(Driver) 执行1遍
    socketStream.map {
      case x => {
        //TODO 代码(Executor) 执行executor个数遍
        x
      }
    }
    //转换,代码(Driver) 执行1遍
    socketStream.transform {
      case rdd => {
        //TODO 代码(Driver) 周期性执行，根据采集周期来执行，每个采集周期执行一遍
        rdd.map {
          case x => {
            //TODO 代码(Executor) executor个遍
            x
          }
        }
      }
    }*/

    context.start()
    context.awaitTermination()
  }
}
