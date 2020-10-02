package com.spark.sql.kudu

import com.google.common.collect.Lists
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.{KuduContext, KuduReadOptions}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @fileName: SparkKuduApp.scala
 * @description: SparkKuduApp.scala类说明
 * @author: by echo huang
 * @date: 2020/9/24 11:12 下午
 */
object SparkKuduApp {
  private val app = new SparkKuduApp
  private val spark: SparkSession = app.spark
  private val context: KuduContext = app.kuduContext

  def main(args: Array[String]): Unit = {

    val schema = new Schema(Lists.newArrayList(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT8).key(true)
      .nullable(false)
      .build()))
    context.createTable("test-kudu", schema, new CreateTableOptions())

    // 查询
    val value: RDD[Row] = context.kuduRDD(spark.sparkContext, "test-kudu", Seq("id"), new KuduReadOptions())
    value.foreach(println)

    // 删除 table
    //    context.deleteTable()
    // 删除数据
    //    context.deleteRows()

    // 查询kudu
    val kuduDF: DataFrame = spark.read.options(Map("kudu.master" -> "localhost:7051", "kudu.table" -> "test"))
      .format("kudu").load

    // 写数据
    kuduDF.write.format("kudu")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("test")
  }
}

class SparkKuduApp {
  val spark: SparkSession = SparkSession.builder()
    .appName("kudu-spark")
    .master("local[8]")
    .getOrCreate()
  val kuduContext = new KuduContext("localhost:7051", spark.sparkContext)
}
