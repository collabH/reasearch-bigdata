package com.spark.rdd.transformations

import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * @fileName: KeyValueOperator.java
  * @description: key-value类型
  * @author: by echo huang
  * @date: 2020-06-27 00:29
  */
object KeyValueOperator extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "keyValueOperator")
    //    partitionByOperator(sc = new SparkContext("local", "keyValueOperator"))
    //    groupByKyeOperator(sc)
    //    reduceByKeyOperator(sc)
    //    aggregateByKeyOperator(sc)
    //    foldByKeyOperator(sc)
    //    combineByKeyOperator(sc)
    //    sortByKeyOperator(sc)
    //    mapValuesOperator(sc)
    //    joinOperator(sc)
    coGroupOperator(sc)
  }

  /**
    * partitionBy算子
    */
  def partitionByOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    rdd.partitionBy(new HashPartitioner(2))
      .collect()
  }

  /**
    * groupByKey算子
    *
    * @param sc
    */
  def groupByKyeOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("aaa", 1), ("bbb", 1), ("ccc", 1), ("ddd", 1), ("ccc", 1), ("bbb", 1)))
    rdd.groupByKey(new HashPartitioner(2))
      .saveAsTextFile("output")
  }

  /**
    * reduceByKey
    *
    * @param sc
    */
  def reduceByKeyOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("aaa", 1), ("bbb", 1), ("ccc", 1), ("ddd", 1), ("ccc", 1), ("bbb", 1)))
    rdd.reduceByKey(_ + _)
      .collect()
      .foreach(println)
  }


  /**
    * aggregateByKey算子
    *
    * @param sc
    */
  def aggregateByKeyOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1), ("b", 4), ("c", 1), ("d", 1)), 2)
    rdd.aggregateByKey(0)(math.max, _ + _)
      .collect()
      .foreach(println)
  }

  /**
    * foldByKey
    *
    * @param sc
    */
  def foldByKeyOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1), ("b", 4), ("c", 1), ("d", 1)), 2)
    rdd.foldByKey(0)(_ + _)
      .collect()
      .foreach(println)
  }

  /**
    * combineByKey
    *
    * @param sc
    * @return
    */
  def combineByKeyOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1), ("b", 4), ("c", 1), ("d", 1)), 2)
    rdd.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .collect()
      .foreach(println)
  }

  /** *
    * sortByKey
    *
    * @param sc
    */
  def sortByKeyOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("aaa", 1), ("bbb", 1), ("ccc", 1), ("ddd", 1), ("ccc", 1), ("bbb", 1)))
    rdd.sortByKey(ascending = false)
      .collect()
      .foreach(println)
  }

  /**
    * mapValues算子
    *
    * @param sc
    */
  def mapValuesOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(Array(("aaa", 1), ("bbb", 1), ("ccc", 1), ("ddd", 1), ("ccc", 1), ("bbb", 1)))
    rdd.mapValues(_ + 1)
      .collect()
      .foreach(println)
  }

  /**
    * join
    *
    * @param sc
    */
  def joinOperator(sc: SparkContext) = {
    val kv1 = sc.makeRDD(List((1, "aa"), (2, "cc"), (3, "vv")))
    val kv2 = sc.makeRDD(List((1, 4), (2, 123), (3, 23)))
    kv1.join(kv2)
      .collect()
      .foreach(println)
  }

  /**
    * coGroup
    *
    * @param sc
    */
  def coGroupOperator(sc: SparkContext) = {
    val kv1 = sc.makeRDD(List((1, "aa"), (2, "cc"), (3, "vv")))
    val kv2 = sc.makeRDD(List((1, 4), (2, 123), (3, 23)))
    kv1.cogroup(kv2)
      .collect()
      .foreach(println)
  }

}
