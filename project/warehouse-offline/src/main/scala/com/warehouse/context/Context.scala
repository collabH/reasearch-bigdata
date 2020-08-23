package com.warehouse.context

import org.apache.spark.sql.SparkSession


/**
  * @fileName: Context.java
  * @description: Context.java类说明
  * @author: by echo huang
  * @date: 2020-08-19 22:10
  */
object Context {
  def getRunContext(appName: String, master: String): SparkSession = {
    SparkSession.builder()
      .enableHiveSupport()
      .config("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.hive.exec.dynamic.partition", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.shuffle.sort.bypassMergeThreshold", "300")
      .config("spark.hive.exec.dynamic.partition", "true")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.default.parallelism", "3")
      .master(master)
      .appName(appName)
      .enableHiveSupport().getOrCreate()
  }

}
