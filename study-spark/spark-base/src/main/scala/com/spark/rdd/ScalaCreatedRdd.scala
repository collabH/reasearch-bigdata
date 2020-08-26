package com.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @fileName: ScalaCreatedRdd.java
  * @description: ScalaCreatedRdd.java类说明
  * @author: by echo huang
  * @date: 2020-06-26 14:43
  */
object ScalaCreatedRdd extends App {
  override def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .registerKryoClasses(Array(classOf[MyClass], classOf[MyClass2]))
    val sc = new SparkContext("local", "createdRdd")
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val arrayRDD = sc.parallelize(List(1, 2, 3, 4, 5))
    listRDD.collect().foreach(print)
    arrayRDD.collect().foreach(print)

  }

}

case class MyClass

case class MyClass2