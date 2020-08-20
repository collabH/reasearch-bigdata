package com.warehouse.function

import com.warehouse.context.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @fileName: UseFunction.java
  * @description: UseFunction.java类说明
  * @author: by echo huang
  * @date: 2020-08-20 22:42
  */
object UseFunction {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context
      .getRunContext("functions", "local[8]")
    import  spark.implicits._
    val frame: DataFrame = spark.table("wh_dwd.dwd_event_log")
    frame
      .select(date_format(current_timestamp(), "yyyyMMdd"))
      .show(1)


    // 下一周一
    frame
      .select(next_day(current_timestamp(), "MO"))
      .show(1)

    // 当前周周一
    frame.select(date_add(next_day(current_timestamp(), "MO"), -7))
        .show(1)

    // 将一列变为set，列转行

    frame.select(collect_set($"l"))
        .show(1)

    spark.close()
  }

}
