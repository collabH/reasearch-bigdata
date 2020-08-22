package com.warehouse.log.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.warehouse.context.Context
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * @fileName: DwdLoadingLog.java
  * @description: DwdLoadingLog.java类说明
  * @author: by echo huang
  * @date: 2020-08-20 21:15
  */
object DwdNotificationLog {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Context.getRunContext("dwd_notification_log", "local[8]")

    import spark.implicits._
    val dwdEvent: Dataset[DwdEventDo] = spark.read.table("wh_dwd.dwd_event_log")
      .where($"eventName" === "notification")
      .as[DwdEventDo]

    dwdEvent.map((event: DwdEventDo) => {
      val json: String = event.eventJson
      val nObject: JSONObject = JSON.parseObject(json)
      val log: DwdNotificationLog = JSON.parseObject(nObject.getString("kv"), classOf[DwdNotificationLog])
      log.ett = nObject.getString("ett")
      log.ln = event.ln
      log.sv = event.sv
      log.os = event.os
      log.g = event.g
      log.mid = event.mid
      log.nw = event.nw
      log.l = event.l
      log.vc = event.vc
      log.hw = event.hw
      log.ar = event.ar
      log.uid = event.uid
      log.t = event.t
      log.la = event.la
      log.md = event.md
      log.vn = event.vn
      log.ba = event.ba
      log.sr = event.sr
      log.serverTime = event.serverTime
      log.ds = event.ds
      log.notifiyType = nObject.getJSONObject("kv").getString("type")
      log
    }).write
      .format("parquet")
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable("wh_dwd.dwd_notification_log")

    spark.close()
  }

}

case class DwdNotificationLog(var ln: String, var sv: String, var os: String, var g: String, var mid: String, var nw: String,
                              var l: String, var vc: String, var hw: String, var ar: String, var uid: String, var t: String, var la: String, var md: String,
                              var vn: String, var ba: String, var sr: String, var serverTime: String,
                              apTime: String, action: String, var notifiyType: String,
                              content: String, var ett: String, var ds: String) {

}
