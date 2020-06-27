package com.spark.rdd.checkpoint

import org.apache.spark.SparkContext

/**
  * @fileName: RDDCheckpoint.java
  * @description: RDD检查点
  * @author: by echo huang
  * @date: 2020-06-27 22:09
  */
object RDDCheckpoint extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","checkpoint")
    sc.setCheckpointDir("checkpoint")
    val rdd = sc.makeRDD(List(1,2,3,4))
    //开启checkpoint
    rdd.checkpoint()
    rdd.foreach(println)
  }

}
