package com.spark.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
/**
 * @fileName: SparkHive.java
 * @description: SparkHive.java类说明
 * @author: by echo huang
 * @date: 2020-06-29 10:27
 */
object SparkHive extends App {
  override def main(args: Array[String]): Unit = {
    //    testYarn()
    //    val sparkBuilder = SparkSession.builder()
    //      .master("local[*]")
    //      .appName("hive")
    //      .config"spark.driver.memory", "4g")
    //      .config("spark.num.executors", "4")
    //      .config("spark.executor.memory", "2g")
    //      .config("spark.executor.cores", "4")
    //      .config("spark.default.parallelism", "10")
    //      .config("spark.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //      .config("spark.sql.parquet.writeLegacyFormat", "true")
    //    val spark = sparkBuilder.enableHiveSupport().getOrCreate()
    //
    //    val frame: DataFrame = spark.sql("select * from forchange_prod.user_orders")
    //    frame.show(20)
    //    spark.close()
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("hive")
      .config("spark.shuffle.manager", "sort")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", 2048)
      .config("spark.sql.files.maxPartitionBytes", 134217728)
      .config("spark.sql.shuffle.partitions", 200)
      .config("spark.sql.inMemoryColumnarStorage.compressed", value = true)
      // 是否启用bypass机制，如果分区数小于该则直接使用hash用于shuffle，前提shuffle map端没有预聚合操作
      .config("spark.shuffle.sort.bypassMergeThreshold", 300)
      .config("spark.shuffle.compress", value = true)
      .config("spark.shuffle.file.buffer", "512k")
      .config("spark.shuffle.io.numConnectionsPerPeer", 5)
      .config("spark.shuffle.spill.compress", value = true)
      .config("spark.io.compression.codec", "snappy")
      .config("spark.driver.memory", "1g")
      .config("spark.num.executors", "3")
      .config("spark.executor.memory", "2g")
      .config("spark.executor.cores", "3")
      .config("spark.default.parallelism", "10")
      .config("spark.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .enableHiveSupport()
      .getOrCreate()
import  spark.implicits._
    //    spark.sql("show databases").show()

    spark.sql("use wh_dwd")
    spark.sql("show tables").show()

    val startLog: DataFrame = spark.table("dwd_start_log")

    startLog
      .select(expr("ba"))
      .select(column("ba"))
      .select(col("ba"))
      .select($"ba")
      .select("ba")
      .select(row_number().over(Window.partitionBy($"ba").orderBy(desc("ds"))).as("rn")).show(1)

    spark.stop()
  }


  def testYarn(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val conf = new SparkConf()
      .setAppName("WordCount")
      // 设置yarn-client模式提交
      .setMaster("yarn")
      // 设置resourcemanager的ip
      .set("yarn.resourcemanager.hostname", "192.168.6.35")
      // 设置executor的个数
      .set("spark.executor.instance", "2")
      // 设置executor的内存大小
      .set("spark.executor.memory", "1024M")
      // 设置提交任务的yarn队列
      .set("spark.yarn.queue", "spark")
      // 设置driver的ip地址
      .set("spark.driver.host", "192.168.6.35")
      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
      .setJars(List(""
      ))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val input = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    input.foreach(print(_))
  }
}
