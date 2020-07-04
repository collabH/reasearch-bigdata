package com.spark.rdd.partition

import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * @fileName: RDDPartitioner.java
  * @description: RDD分区器
  * @author: by echo huang
  * @date: 2020-06-27 23:24
  */
object RDDPartitioner extends App {

  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "partitioner")
    getPartitioner(sc)
  }

  def getPartitioner(sc: SparkContext) = {
    val rdd = sc.makeRDD(List((1,"1"),(2,"2")))
    val hashRdd = rdd.partitionBy(new HashPartitioner(2))
    println(hashRdd.partitioner.get)
  }

}
