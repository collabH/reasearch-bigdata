package com.warehouse.log.ads

import com.warehouse.context.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @fileName: AdsUvCount.java
  * @description: AdsUvCount.java类说明
  * @author: by echo huang
  * @date: 2020-08-22 12:29
  */
object AdsUvCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("ads_uv_count", "local[8]")


    val dwsUvDetails: DataFrame = spark.table("wh_dwd.dwd_start_log")

    import spark.implicits._

    dwsUvDetails
      .groupBy($"ds", $"mid")
      .agg($"mid", count("mid").as("uv"))
      .select("uv", "mid")
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable("wh_ads.ads_uv_count")

    spark.close()

  }
}
