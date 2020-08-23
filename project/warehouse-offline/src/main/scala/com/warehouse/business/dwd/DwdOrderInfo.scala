package com.warehouse.business.dwd

import com.warehouse.context.Context
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @fileName: DwdOrderInfo.java
  * @description: DwdOrderInfo.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 14:03
  */
object DwdOrderInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_order_info", "local[8]")

    spark.sql(
      """create table if not exists bussine_dwd.dwd_order_info (
        |      `id` string COMMENT '',
        |    `total_amount` decimal(10,2) COMMENT '',
        |    `order_status` string COMMENT ' 1 2 3 4 5',
        |    `user_id` string COMMENT 'id',
        |    `payment_way` string COMMENT '',
        |    `out_trade_no` string COMMENT '',
        |    `create_time` string COMMENT '',
        |    `operate_time` string COMMENT ''
        |    )
        |    PARTITIONED BY (`dt` string)
        |    """.stripMargin)


    import spark.implicits._
    spark.sql("use bussine_source")
    val odsOrderInfo: DataFrame = spark.table("ods_order_info")
      .filter($"id".isNotNull).filter($"total_amount".geq(0))

    odsOrderInfo.write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("bussine_dwd.dwd_order_info")

    spark.close()


  }
}
