package com.warehouse.business.dwd

import com.warehouse.context.Context
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @fileName: DwdOrderDetails.java
  * @description: DwdOrderDetails.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 15:24
  */
object DwdUserInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_user_info", "local[8]")

    spark.sql("use bussine_dwd")

    spark.sql(
      """
        |create table if not exists dwd_user_info(
        |id string,
        |name string,
        |birthday string,
        |gender string,
        |email string,
        |user_level string,
        |create_time string)
        |partitioned by(dt string)
      """.stripMargin)

    import spark.implicits._

    spark.table("bussine_source.ods_user_info")
      .filter($"id".isNotNull)
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("dwd_user_info")

    spark.close()
  }

}
