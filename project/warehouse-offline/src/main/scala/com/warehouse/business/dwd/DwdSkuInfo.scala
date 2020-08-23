package com.warehouse.business.dwd

import com.warehouse.context.Context
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * @fileName: DwdSkuInfo.java
  * @description: DwdSkuInfo.java类说明
  * @author: by echo huang
  * @date: 2020-08-23 15:43
  */
object DwdSkuInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_sku_info", "local[8]")


    spark.sql("use bussine_dwd")

    spark.sql(
      """
        |create table dwd_sku_info(
        |id string,
        |spu_id string,
        |price decimal(10,2),
        |sku_name string,
        |sku_desc string,
        |weight string,
        |tm_id string,
        |category3_id string,
        |category2_id string,
        |category1_id string,
        |category1_name string,
        |category2_name string,
        |category3_name string,
        |create_time string)
        |partitioned by(dt string)
      """.stripMargin)

    import spark.implicits._
    spark.sql("use bussine_source")

    // 读取ods_sku_info表
    val odsSkuInfo: Dataset[Row] = spark.table("ods_sku_info")
      .where($"id".isNotNull)
      .where($"price".geq(0))
    val category1: Dataset[Row] = spark.table("ods_base_category1")
      .where($"id".isNotNull)

    val category2: Dataset[Row] = spark.table("ods_base_category2")
      .where($"id".isNotNull)

    val category3: Dataset[Row] = spark.table("ods_base_category3")
      .where($"id".isNotNull)

    odsSkuInfo.join(category3, $"category3_id".isNotNull && $"category3_id".equalTo(category3("id")))
      .join(category2, $"category2_id".isNotNull && $"category2_id".equalTo(category2("id")))
      .join(category1, $"category1_id".isNotNull && $"category1_id".equalTo(category1("id")))
      .select(odsSkuInfo("id"), odsSkuInfo("spu_id"), odsSkuInfo("price"), odsSkuInfo("sku_name"),
        odsSkuInfo("sku_desc"), odsSkuInfo("weight"), odsSkuInfo("tm_id"), $"category3_id", $"category2_id", $"category1_id", category3("name").as("category3_name"),
        category2("name").as("category2_name"), category1("name").as("category1_name"), $"create_time", odsSkuInfo("dt"))
      .write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .insertInto("bussine_dwd.dwd_sku_info")

    spark.close()
  }

}
