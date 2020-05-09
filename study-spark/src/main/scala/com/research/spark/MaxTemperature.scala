package com.research.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @fileName: JavaMaxTemperature.java
  * @description: JavaMaxTemperature.java类说明
  * @author: by echo huang
  * @date: 2020-04-12 21:38
  */
object MaxTemperature {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("max temperature")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile(args(0))
      .map(_.split(","))
      .filter(_ != "0")
      .saveAsTextFile(args(1))
  }
}
