package com.spark.sql.udf

import org.apache.spark.sql.SparkSession

/**
  * @fileName: SparkUDF.java
  * @description: UDF
  * @author: by echo huang
  * @date: 2020-06-30 21:02
  */
object SparkUDF extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("udf")
      .master("local[*]")
      .getOrCreate()
    //注册UDF
    spark.udf.register("addName", (x: String) => s"Name$x")
    import spark.implicits._
    val df = spark.sparkContext.makeRDD(List("hsm", "wy")).toDF("name")
    df.createTempView("student")
    //使用函数
    val result = spark.sql("select addName(name) from student")
    result.show()
    //释放资源
    spark.close()
  }

}
