package com.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @fileName: SparkSQLDataFrame.java
  * @description: DataFrame学习
  * @author: by echo huang
  * @date: 2020-06-28 22:50
  */
object SparkSQLDataFrame extends App {
  override def main(args: Array[String]): Unit = {
    //    rddToDf()
    //    createTmpVie(createDataFrame())
    createDataFreamNew()
  }

  /**
    * 创建DataFrame
    */
  def createDataFrame() = {
    val df = SparkSession.builder()
      .appName("dataframe")
      .master("local")
      .getOrCreate()
    df

  }

  def readJson() = {
    val df = createDataFrame()
    val jsonValue = df.read
      .json("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.json")
    jsonValue
  }

  def createTmpVie(df: SparkSession) = {
    val json = readJson()
    json.createTempView("student")
    df.sql("select * from student").show()
  }

  def createGlobalTempViewVie(df: SparkSession) = {
    val json = readJson()
    json.createGlobalTempView("student")
    df.sql("select * from student").show()
  }

  def createDataFreamNew() = {
    val spark = createDataFrame()
    val jsonValue = readJson()
    jsonValue.createOrReplaceGlobalTempView("student")
    spark.sql("select * from global_temp.student").show()
    //创建一个新范围的session
    val session = spark.newSession()
    session.sql("select * from global_temp.student").show()
  }

  //-------DSL
  def dsl() = {
    val df = createDataFrame()
    val valueDF = df.emptyDataFrame
    //打印表结构
    valueDF.printSchema()
    //查询列数据
    valueDF.select("name").show()
    valueDF.filter("name")
    valueDF.groupBy("age")
    valueDF.count()
  }


  //RDD to DF
  def rddToDf(spark: SparkSession) = {
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List(1, 2, 3))
    val df: DataFrame = rdd.toDF("id")
    df.show()
  }
}
