package com.warehouse.business.dwd

import com.warehouse.context.Context
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @fileName: DwdOrderDetails.java
  * @description: DwdOrderDetails.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 15:24
  */
object DwdPaymentInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_payment_info", "local[8]")

    spark.sql("use bussine_dwd")

    spark.sql(
      """
        |create table if not exists dwd_payment_info(
        |id string,
        |out_trade_no string,
        |order_id string,
        |user_id string,
        |alipay_trade_no string,
        |total_amount decimal(16,2),
        |subject string,
        |payment_type string,
        |payment_time string)
        |partitioned by(dt string)
      """.stripMargin)

    import spark.implicits._

    spark.table("bussine_source.ods_payment_info")
      .filter($"id".isNotNull)
      .filter($"total_amount".geq(0))
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("dwd_payment_info")

    spark.close()
  }

}
