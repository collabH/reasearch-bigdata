package com.spark.rdd.transformations

import org.apache.spark.SparkContext

/**
  * @fileName: SparkRDDPractice.java
  * @description: 求每个省份的点击量
  * @author: by echo huang
  * @date: 2020-06-27 16:28
  */
object SparkRDDPractice extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "rddPractice")
    val rdd = sc.makeRDD(List(("河北", "xxxx"), ("河北", "yyyy"), ("河北", "xxxx"), ("河南", "xxxx"), ("河南", "yyyy"), ("江西", "yyyy")))
    rdd.map(k => (k._1 + "-" + k._2, 1))
      .reduceByKey(_ + _)
      .map(k => {
        val key = k._1.split("-")
        (key.apply(0), key.apply(1), k._2)
      }).sortBy(k=>k._3,ascending = false)
      .groupBy(k => k._1)
      .collect()
      .foreach(println)
  }
}
