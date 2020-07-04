package com.spark.streaming.window

/**
  * @fileName: ScalaWindowFunction.java
  * @description: ScalaWindowFunction.java类说明
  * @author: by echo huang
  * @date: 2020-07-03 13:57
  */
object ScalaWindowFunction {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6,7,8)
    //滑动窗口，size为窗口大小，step为步长，增长的幅度
    val iterator: Iterator[List[Int]] = list.sliding(3,3)
    for (elem <- iterator) {
      println(elem.mkString(","))
    }
  }
}
