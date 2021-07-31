package com.spark.sql

import com.google.common.collect.Lists
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.days
import org.apache.spark.sql.{DataFrame, DataFrameWriterV2, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * @fileName: IcebergWriter.scala
 * @description: IcebergWriter.scala类说明
 * @author: huangshimin
 * @date: 2021/7/25 7:56 下午
 */
object IcebergWriter extends App {




  override def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("iceberg-write")
      .master("local")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .enableHiveSupport().getOrCreate()
    spark.sql("use iceberg_db")
    spark.sql("create table if not exists iceberg_write(id int,name string,ts timestamp)using iceberg partitioned by" +
      "(days(ts)) ")


    //    df.writeTo("iceberg_db.test1")
    //      .tableProperty("write.format.default", "parquet")
    //      .partitionedBy(col("level"), days(col("ts")))
    //      .createOrReplace()
    val df: DataFrame = spark.createDataFrame(Seq(Test(5, "hsm2",Timestamp.valueOf(LocalDateTime.now()))))
    val value: DataFrameWriterV2[Row] = df.writeTo("iceberg_db.iceberg_write")
    // append
    //    value
    //      .append()
    import spark.implicits._
//    value.create()

    value.overwritePartitions()

    spark.close()
  }
}

case class Test(id: Integer, name: String,ts:Timestamp) {
}
