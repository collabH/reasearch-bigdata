package com.warehouse.log.dwd

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.warehouse.context.Context
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @fileName: DwdEventLog.java
  * @description: DwdEventLog.java类说明
  * @author: by echo huang
  * @date: 2020-08-19 23:53
  */
object DwdEventLog {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwdEventLog", "local[4]")

    import spark.implicits._
    val odsEventLog: Dataset[OdsDo] = spark.read.table("wh_ods.ods_event_log")
      .as[OdsDo]

    odsEventLog.filter(odsEventLog("line").isNotNull)
      .flatMap((data: OdsDo) => {
        val arr: Array[String] = data.line.split("\\|")
        val list: mutable.ListBuffer[DwdEventDo] = new ListBuffer[DwdEventDo]()
        if (arr != null || StringUtils.isEmpty(arr(1))) {
          val jsonObject: JSONObject = JSON.parseObject(arr(1))
          val array: JSONArray = jsonObject.getJSONArray("et")
          if (array != null) {
            array.forEach((obj: Object) => {
              val dwdEventDo: DwdEventDo = JSON.parseObject(jsonObject.getString("cm"), classOf[DwdEventDo])
              val en: JSONObject = obj.asInstanceOf[JSONObject]
              dwdEventDo.eventName = en.getString("en")
              dwdEventDo.eventJson = en.toJSONString
              dwdEventDo.ds = data.ds
              dwdEventDo.serverTime = arr(0)
              list.append(dwdEventDo)
            })
          }
        }
        list
      }).write
        .format("parquet")
        .mode(saveMode = SaveMode.Overwrite)
        .saveAsTable("wh_dwd.dwd_event_log")
    spark.close()

  }

}

case class DwdEventDo(ln: String, sv: String, os: String, g: String, mid: String, nw: String,
                      l: String, vc: String, hw: String, ar: String, uid: String, t: String, la: String, md: String,
                      vn: String, ba: String, sr: String, var serverTime: String, var eventName: String, var eventJson: String, var ds: String)