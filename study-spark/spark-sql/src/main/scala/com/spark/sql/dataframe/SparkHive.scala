package com.spark.sql.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @fileName: SparkHive.java
  * @description: SparkHive.java类说明
  * @author: by echo huang
  * @date: 2020-06-29 10:27
  */
object SparkHive extends App {
  override def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
      .master("local[*]")
      .appName("hive")
      .config("spark.driver.memory", "4g")
      .config("spark.num.executors", "4")
      .config("spark.executor.memory", "2g")
      .config("spark.executor.cores", "4")
      .config("spark.default.parallelism", "10")
      .config("spark.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
    val spark = sparkBuilder.enableHiveSupport().getOrCreate()

    val frame: DataFrame = spark.sql("select * from forchange_prod.user_orders")
    frame.show(20)
    spark.close()
  }
}
