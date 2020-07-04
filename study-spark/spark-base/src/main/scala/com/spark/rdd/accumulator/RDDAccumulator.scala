package com.spark.rdd.accumulator

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/**
  * @fileName: RDDAccumulator.java
  * @description: RDD累加器
  * @author: by echo huang
  * @date: 2020-06-28 13:24
  */
object RDDAccumulator extends App {

  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "accumulator")
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    //存在 spark shuffle过程
    println(rdd.reduce(_ + _))

    //无法将executor数据传递会driver，因此sum最终在driver端打印为0
    var sum = 0
    rdd.foreach(i => sum = sum + i)
    println(sum)

    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(i => sumAcc.add(i))
    println(sumAcc.value)

    //创建累加器
    val accumulator = new WordAccumulator()
    //注册累加器
    sc.register(accumulator)
    val valueRDD = sc.makeRDD(List("hadoop", "test", "hello"))

    valueRDD.foreach(data => accumulator.add(data))
    println(accumulator.value)
    //释放资源
    sc.stop()

  }
}

//自定义累加器
//1.继承AccumulatorV2
//2.根据特定业务指定Driver到Executor的INPUT数据和Executor返回Driver的Output数据
//3.创建累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  //存储数据的list
  var list = new util.ArrayList[String]()


  /**
    * 当前累加器是否为初始化状态
    *
    * @return
    */
  override def isZero: Boolean = {
    list.isEmpty
  }

  /**
    * 拷贝当前累加器
    *
    * @return
    */
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    val accumulator = new WordAccumulator()
    accumulator
  }

  /**
    * 重置累加器数据
    */
  override def reset(): Unit = list.clear()

  /**
    * 向累加器汇总增加数据
    *
    * @param v
    */
  override def add(v: String): Unit = {
    val bool: Boolean = v.contains("h")
    if (bool) {
      this.list.add(v)
    }
  }

  /**
    * 合并累加器
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  /**
    * 返回累加器的值
    *
    * @return
    */
  override def value: util.ArrayList[String] = {
    list
  }
}