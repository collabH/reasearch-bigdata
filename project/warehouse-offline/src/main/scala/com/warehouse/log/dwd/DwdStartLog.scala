package com.warehouse.log.dwd

import com.alibaba.fastjson.JSON
import com.warehouse.context.Context
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * @fileName: OdsStartLog.java
  * @description: OdsStartLog.java类说明
  * @author: by echo huang
  * @date: 2020-08-19 22:09
  */
object DwdStartLog {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("ods_start_log", "local[4]")

    import spark.implicits._
    val dwdStartLog: Dataset[OdsDo] = spark.read
      .table("wh_ods.ods_start_log")
      .as[OdsDo]

    dwdStartLog.map(
      (data: OdsDo) => {
        val logDo: StartLogDo = JSON.parseObject(data.line, classOf[StartLogDo])
        logDo.ds = data.ds
        logDo
      }).toDF().write.format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable("wh_dwd.dwd_start_log")
    spark.close()

  }
}

case class OdsDo(line: String, ds: String) {

}

case class StartLogDo(action: String,
                      ar: String,
                      ba: String,
                      detail: String,
                      en: String,
                      entry: String,
                      extend1: String,
                      g: String,
                      hw: String,
                      l: String,
                      la: String,
                      ln: String,
                      loadingTime: String,
                      md: String,
                      mid: String,
                      nw: String,
                      openAdType: String,
                      os: String,
                      sr: String,
                      sv: String,
                      t: String,
                      uid: String,
                      vc: String,
                      vn: String,
                      var ds: String)