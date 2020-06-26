package com.spark.rdd

import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * @fileName: KeyValueOperator.java
  * @description: key-value类型
  * @author: by echo huang
  * @date: 2020-06-27 00:29
  */
object KeyValueOperator extends App {
  override def main(args: Array[String]): Unit = {
    partitionByOperator(sc = new SparkContext("local","keyValueOperator"))
  }

  /**
    * partitionBy算子
    */
  def partitionByOperator(sc:SparkContext) = {
    val rdd = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    rdd.partitionBy(new HashPartitioner(2))
      .collect()
  }
}
