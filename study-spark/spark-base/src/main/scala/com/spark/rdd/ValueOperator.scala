package com.spark.rdd

import java.util.Objects

import org.apache.spark.SparkContext

/**
  * @fileName: Operator.java
  * @description: Operator.java类说明
  * @author: by echo huang
  * @date: 2020-06-26 15:39
  */
object ValueOperator extends App {

  override def main(args: Array[String]): Unit = {
    //Driver
    val sc = new SparkContext("local", "operator")
    //    mapOperator(sc)
    //    filterOperator(sc)
    //    mapPartitionOperator(sc)
    //    mapPartitionWithIndexOperator(sc)
    //    flatMapOperator(sc)
    //    glomOperator(sc)
    //    groupByOperator(sc)
    //    sampleOperator(sc)
    //    distinctOperator(sc)
    coalesceOperator(sc)
  }

  /**
    * coalesce缩减分区数
    *
    * @param sc
    */
  def coalesceOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 4)
    rdd.coalesce(3,shuffle = true)
      .saveAsTextFile("output")
  }

  /**
    * sample(withReplacement, fraction, seed)使用
    *
    * @param sc
    */
  def sampleOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    //抽样
    rdd.sample(withReplacement = false, 0.5, 1)
      .collect()
      .foreach(println)
  }

  /**
    * distinct算子
    *
    * @param sc
    */
  def distinctOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 1, 2, 4, 6))
    rdd.distinct().collect()
      .foreach(println)
  }

  /**
    * groupBy算子
    *
    * @param sc
    */
  def groupByOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    rdd.groupBy(x =>
      x % 2 == 0
    ).collect()
      .foreach(println)
  }

  /**
    * map算子使用
    *
    * @param sc
    */
  def mapOperator(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    rdd.map(n => {
      //executor执行
      s"$n:hello"
    }).collect().foreach(println)
  }

  /**
    * glom
    *
    * @param sc
    */
  def glomOperator(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    rdd.glom().
      collect()
      .foreach(array => println(array.mkString(",")))

  }

  /**
    * mapPartition算子使用
    *
    * @param sc
    */
  def mapPartitionOperator(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    rdd.mapPartitions(datas => {
      datas.map(_ + "hello")
    }).collect()
      .foreach(println)
  }

  /**
    * mapPartitionWithIndex算子使用
    *
    * @param sc
    */
  def mapPartitionWithIndexOperator(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    rdd.mapPartitionsWithIndex((index, datas) => {
      datas.map(_ + "Hello" + index)
    })
      .collect()
      .foreach(println)
  }


  /**
    * flatMap算子使用
    *
    * @param sc
    */
  def flatMapOperator(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    rdd.flatMap(v =>
      List(v, 1)
    )
      .collect()
      .foreach(println)
  }

  /**
    * filter算子使用
    *
    * @param sc
    */
  def filterOperator(sc: SparkContext): Unit = {
    sc.makeRDD(List("hello", null, "world"))
      .filter(v => {
        Objects.nonNull(v)
      })
      .collect()
      .foreach(println)
  }
}
