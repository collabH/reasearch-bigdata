package com.warehouse.log.ads

import com.warehouse.context.Context
import com.warehouse.log.dwd.StartLogDo
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * @fileName: AdsInsertMidCount.java
  * @description: AdsInsertMidCount.java类说明
  * @author: by echo huang
  * @date: 2020-08-22 14:10
  */
object AdsInsertMidCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("ads_insert_mid_count", "local[8]")
    import spark.implicits._

    val value: Dataset[StartLogDo] = spark.table("wh_dwd.dwd_start_log").as[StartLogDo]


    value.map((data: StartLogDo) => {
      data.ds="2020-08-20"

      data
    }).limit(200).write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("wh_dwd.dwd_start_log")


    spark.close()

  }

}
