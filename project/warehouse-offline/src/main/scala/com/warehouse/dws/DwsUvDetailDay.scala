package com.warehouse.dws

import com.warehouse.context.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @fileName: DwsUvDetailDay.java
  * @description: 分析每日活跃设备
  * @author: by echo huang
  * @date: 2020-08-20 22:57
  */
object DwsUvDetailDay {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dws_uv_detail_day", "local[8]")

    import spark.implicits._
    val startLog: DataFrame = spark.table("wh_dwd.dwd_start_log")
      .groupBy($"mid")
      .agg(concat_ws(",", collect_set($"uid")).as("uid"),
        concat_ws(",", collect_set($"vc")).as("vc"),
        concat_ws(",", collect_set($"vn")).as("vn"),
        concat_ws(",", collect_set($"l")).as("l"),
        concat_ws(",", collect_set($"sr")).as("sr"),
        concat_ws(",", collect_set($"os")).as("os"),
        concat_ws(",", collect_set($"ar")).as("ar"),
        concat_ws(",", collect_set($"md")).as("md"),
        concat_ws(",", collect_set($"ba")).as("ba"),
        concat_ws(",", collect_set($"sv")).as("sv"),
        concat_ws(",", collect_set($"g")).as("g"),
        concat_ws(",", collect_set($"hw")).as("hw"),
        concat_ws(",", collect_set($"t")).as("t"),
        concat_ws(",", collect_set($"ln").as("ln"),
          concat_ws(",", collect_set($"la"))).as("la"))

    startLog
      .select("*")
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable("wh_dws.dws_uv_detail_day")
    spark.close()

  }
}
