package com.warehouse.business.dwd

import com.warehouse.context.Context
import org.apache.spark.sql.{SaveMode, SparkSession}
import  org.apache.spark.sql.functions._;
/**
  * @fileName: DwdOrderDetails.java
  * @description: DwdOrderDetails.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 15:24
  */
object DwdOrderDetails {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_order_details", "local[8]")


    spark.sql(
      """
        |create table if not exists bussine_dwd.dwd_order_detail(
        |id string,
        |order_id string,
        |user_id string,
        |sku_id string,
        |sku_name string,
        |order_price string,
        |sku_num string,
        |create_time string)
        |partitioned by(dt string)
      """.stripMargin)

    import spark.implicits._

    spark.table("bussine_source.ods_order_detail")
      .filter($"id".isNotNull)
      .filter($"sku_num".geq(0))
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("bussine_dwd.dwd_order_detail")

    spark.close()
  }

}
