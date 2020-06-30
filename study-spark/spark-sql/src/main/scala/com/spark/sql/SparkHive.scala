package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @fileName: SparkHive.java
  * @description: SparkHive.java类说明
  * @author: by echo huang
  * @date: 2020-06-29 10:27
  */
object SparkHive extends App {
  override def main(args: Array[String]): Unit = {
    testYarn()
    //    val sparkBuilder = SparkSession.builder()
    //      .master("local[*]")
    //      .appName("hive")
    //      .config("spark.driver.memory", "4g")
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
