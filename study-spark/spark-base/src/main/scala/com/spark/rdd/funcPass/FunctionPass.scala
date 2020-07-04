package com.spark.rdd.funcPass

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @fileName: FunctionPass.java
  * @description: FunctionPass.java类说明
  * @author: by echo huang
  * @date: 2020-06-27 17:19
  */
object FunctionPass extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "funcPass")
    val rdd = sc.makeRDD(List("hadoop", "spark", "java", "scala", "python"))
    val search = new Search("hadoop")
    val match1 = search.getMatch1(rdd)
    match1.collect()
      .foreach(println)
    //释放资源
    sc.stop()
  }
}

/**
  * 传递的方法 序列化
  *
  * @param query
  */
class Search(query: String) extends Serializable {
  /**
    * 匹配方法
    * @param s
    * @return
    */
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]): RDD[String] = {
    //传递方法
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }
}