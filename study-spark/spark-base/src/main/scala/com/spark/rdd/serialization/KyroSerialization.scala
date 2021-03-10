package com.spark.rdd.serialization

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random


/**
 * @fileName: KyroSerialization.scala
 * @description: KyroSerialization.scala类说明
 * @author: by echo huang
 * @date: 2021/3/9 10:44 下午
 */
object KyroSerialization {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("testKryo")
    sparkConf.registerKryoClasses(Array(classOf[Test]))
    val sc = new SparkContext(sparkConf)
    val ids = Array("zhangsan", "lisi", "wangwu")
    val names = Array("zhangsan", "lisi", "wangwu")
    val list = new mutable.ArraySeq[Test](1000000)
    for (i <- 1 to 1000000) {
      val str: Test = Test(ids(Random.nextInt(2)), names(Random.nextInt(2)))
      list.update(i - 1, str)
    }
    val value: RDD[Test] = sc
      .makeRDD(list)

    value.persist(StorageLevels.MEMORY_ONLY_SER)
    println(value.count())
    Thread.sleep(10 * 1000 * 5)
    sc.stop()
  }


  case class Test(id: String, name: String)

}
