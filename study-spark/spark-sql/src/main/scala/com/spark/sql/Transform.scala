package com.spark.sql

import com.spark.sql.SparkSQLDataSet.Person
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @fileName: Transform.java
  * @description: Transform.java类说明
  * @author: by echo huang
  * @date: 2020-06-30 20:12
  */
class Transform {

  def get() = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("transform")
      .getOrCreate()
    spark
  }
}

object Transform {
  def main(args: Array[String]): Unit = {
    val spark = new Transform().get()
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List(("hsm", 24), ("wy", 24)))
    //转换成df
    val df = rdd.toDF("name", "age")
    df.show()

    //转换成ds
    val ds: Dataset[Person] = df.as[Person]
    ds.select("name", "age").show()
    //转换df
    val df1 = ds.toDF()
    df1.show()
    //转换RDD
    val rdd1 = df1.rdd
    rdd1.collect()
      .foreach(println)

  }
}
