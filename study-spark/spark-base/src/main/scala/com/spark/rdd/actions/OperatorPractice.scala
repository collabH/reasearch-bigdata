package com.spark.rdd.actions

import com.google.common.base.Preconditions
import org.apache.spark.SparkContext

/**
  * @fileName: OperatorPractice.java
  * @description: action算子
  * @author: by echo huang
  * @date: 2020-06-27 16:54
  */
object OperatorPractice extends App {

  override def main(args: Array[String]): Unit = {
    Preconditions.checkNotNull(null)

//    val sc = new SparkContext("local", "action")
//    reduceOperator(sc)
//    collectOperator(sc)
//    aggregateOperator(sc)
  }

  /**
    * reduce
    *
    * @param sc
    */
  def reduceOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    println(rdd.reduce(_ + _))
  }


  /**
    * collect
    *
    * @param sc
    */
  def collectOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    rdd.collect().foreach(println)
  }

  /**
    * count
    *
    * @param sc
    */
  def countOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    println(rdd.count())
  }


  /**
    * aggregate算子
    * @param sc
    */
  def aggregateOperator(sc: SparkContext) = {
    val rdd = sc.makeRDD(1 to 10,2)
    println(rdd.aggregate(0)(_ + _, _ + _))
  }
}
