package com.spark.rdd.broadcast

import org.apache.spark.SparkContext

/**
  * @fileName: RDDBroadcast.java
  * @description: RDD广播变量
  * @author: by echo huang
  * @date: 2020-06-28 13:24
  */
object RDDBroadcast extends App {


  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "broadcast")
    val list = sc.makeRDD(List((1, 2), (2, 4), (3, 4)))

    //广播变量减少数据传输，并且也无需因为join产生的shuffle过程或者向每一个Executor传送数据的问题
    //1.构建广播变量
    val broadcast = sc.broadcast(List((1, "name"), (2, "hsm")))
    val resultRdd = list.map {
      case (key, value) => {
        var v2: Any = null
        //使用广播变量
        for (elem <- broadcast.value) {
          if (key == elem._1) {
            v2 = elem._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRdd.foreach(println)
  }
}
