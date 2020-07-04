package com.spark.rdd.cache

import org.apache.spark.SparkContext

/**
  * @fileName: RDDCache.java
  * @description: RDD缓存
  * @author: by echo huang
  * @date: 2020-06-27 21:53
  */
object RDDCache extends App {

  override def main(args: Array[String]): Unit = {

  }

  def cache(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array("hsm"))
    //加上cache
    val mapRDD = rdd.map(_ + System.currentTimeMillis()).cache()
    //每次的时间戳都相同
    mapRDD.collect()
  }

  def persist(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array("hsm"))
    //加上persist
    val mapRDD = rdd.map(_ + System.currentTimeMillis()).persist()
    //每次的时间戳都相同
    mapRDD.collect()
  }

}
