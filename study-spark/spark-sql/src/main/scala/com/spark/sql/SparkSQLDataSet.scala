package com.spark.sql

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @fileName: SparkSQLDataSet.java
  * @description: DataSet API
  * @author: by echo huang
  * @date: 2020-06-30 00:04
  */
object SparkSQLDataSet extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("dataset")
      .getOrCreate()
        createDataSet(spark)
    //    rddToDataSet(spark)
    //    dataFrameToDataset(spark)
//    SeqToDataSet(spark)
  }

  case class Person(name: String, age: Long)

  def createDataSet(spark: SparkSession) = {
    val value: RDD[Person] = spark.sparkContext.makeRDD(Seq(Person("name", 1)))
    //隐式转换
    import spark.implicits._
    val ds = value.toDS()
    ds.show()
    ds.select("name").show()
  }

  def rddToDataSet(spark: SparkSession) = {
    val sc = spark.sparkContext
    //隐式转换
    val rdd = sc.textFile("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.json")
    val mapper = new ObjectMapper()
    import spark.implicits._
    val ds = rdd.map(t => {
      val value: Person = mapper.reader()
        .readValue(t)
      value
    }).toDS()
    ds.show()
  }

  def dataFrameToDataset(spark: SparkSession) = {
    //隐式转换
    import spark.implicits._
    val ds = spark.read
      .json("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.json")
      .as[Person]
    ds.show()
  }


  def SeqToDataSet(spark: SparkSession) = {
    import spark.implicits._
    Seq(1, 2, 3, 4).toDF("id").show()
  }
}


