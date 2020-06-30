package com.spark.sql.readsave

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @fileName: SparkSqlReadData.java
  * @description: 读取数据
  * @author: by echo huang
  * @date: 2020-06-30 23:17
  */
object SparkSqlReadData extends App {
  override def main(args: Array[String]): Unit = {
    val sparkSqlReadData = new SparkSqlReadData
    //    loadData(sparkSqlReadData)
    //    assignFormatRead(sparkSqlReadData)
    //    saveData(sparkSqlReadData)
    //    assignFormatSave(sparkSqlReadData)
    saveTable(sparkSqlReadData)
  }


  /**
    * 读取默认的parqeut格式文件
    */
  def loadData(sparkSqlReadData: SparkSqlReadData) = {
    val spark = sparkSqlReadData.get()
    val df: DataFrame = spark.read.load("file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/output")
    df.show()
    df.printSchema()
    df
  }

  /**
    * 指定特定格式读取
    *
    * @param sparkSqlReadData
    */
  def assignFormatRead(sparkSqlReadData: SparkSqlReadData) = {
    val spark = sparkSqlReadData.get()
    val df = spark.read.format("json").load("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.json")
    df.show()
    df
  }

  /**
    * 默认格式保存文件
    *
    * @param sparkSqlReadData
    */
  def saveData(sparkSqlReadData: SparkSqlReadData) = {
    val df = loadData(sparkSqlReadData)
    df.write.save("output")
  }

  /**
    *
    * @param sparkSqlReadData
    */
  def assignFormatSave(sparkSqlReadData: SparkSqlReadData) = {
    val df = assignFormatRead(sparkSqlReadData)
    df.write.mode(SaveMode.Overwrite).format("json").save("output")
  }

  /**
    * 存储成table格式
    *
    * @param sparkSqlReadData
    */
  def saveTable(sparkSqlReadData: SparkSqlReadData) = {
    val df = assignFormatRead(sparkSqlReadData)
    df.write.mode(SaveMode.Overwrite).saveAsTable("user")
  }
}

class SparkSqlReadData {
  def get() = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("read")
      .getOrCreate()
    spark
  }
}
