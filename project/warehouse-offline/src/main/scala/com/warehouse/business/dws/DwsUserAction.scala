package com.warehouse.business.dws

import com.warehouse.context.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @fileName: DwsUserAction.java
  * @description: DwsUserAction.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 16:18
  */
object DwsUserAction {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dws_user_action", "local[8]")


    spark.sql("create database if not exists bussine_dws")
    spark.sql("use bussine_dws")

    spark.sql(
      """
        |create table if not exists dws_user_action(
        |user_id string,
        |order_count string,
        |order_amount decimal(16,2),
        |payment_count bigint,
        |payment_amount decimal(16,2))
        |partitioned by(dt string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
      """.stripMargin)

    spark.sql("use bussine_dwd")
    import spark.implicits._

    val dwdOrderInfo: DataFrame = spark.table("dwd_order_info")
      .groupBy($"user_id")
      .agg(count("id").as("order_count"),
        sum("total_amount").as("order_amount"))

    val dwsPaymentInfo: DataFrame = spark.table("dwd_payment_info")
      .groupBy("user_id")
      .agg(count("id").as("payment_count"), sum("total_amount").as("payment_amount"))

    // 根据user_id join

    dwdOrderInfo.join(dwsPaymentInfo, Seq("user_id"), "left")
      .select($"user_id", $"order_count", $"order_amount", $"payment_count", $"payment_amount", date_format(current_timestamp(), "yyyy-MM-dd").as("dt"))
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("bussine_dws.dws_user_action")

    spark.close()

  }

}
