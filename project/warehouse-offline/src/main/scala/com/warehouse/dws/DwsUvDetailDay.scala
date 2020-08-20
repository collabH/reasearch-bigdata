package com.warehouse.dws

import com.warehouse.context.Context
import org.apache.commons.collections.collection.TypedCollection
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * @fileName: DwsUvDetailDay.java
  * @description: 分析每日活跃设备
  * @author: by echo huang
  * @date: 2020-08-20 22:57
  */
object DwsUvDetailDay {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dws_uv_detail_day", "local[8]")

    val startLog: DataFrame = spark.table("wh_dwd.dwd_start_log")
    import  spark.implicits._

    startLog.select(startLog("mid"),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"vc")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid")),
      concat_ws(",",collect_set($"uid"))
    )
  }
}
