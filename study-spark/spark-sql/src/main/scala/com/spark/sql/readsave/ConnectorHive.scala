package com.spark.sql.readsave

import org.apache.parquet.hadoop.codec.SnappyCodec
import org.apache.spark.sql.SparkSession

/**
  * @fileName: ConnectorHive.java
  * @description: spark connect hive
  * @author: by echo huang
  * @date: 2020-07-01 13:00
  */
object ConnectorHive extends App {

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("hive")
      .config("io.compression.codecs",classOf[SnappyCodec].getName)
      .enableHiveSupport()
      .getOrCreate()
//    insertTable(spark)
    queryTable(spark)
    spark.close()
  }

  def createTable(spark: SparkSession) = {
    spark.sql("create table test(id bigint,name string)")
  }

  def insertTable(spark: SparkSession) = {
    spark.sql("insert into table test values(1,'hsm')")
  }

  def queryTable(spark: SparkSession) = {
    spark.sql("select * from test").show()
  }
}
