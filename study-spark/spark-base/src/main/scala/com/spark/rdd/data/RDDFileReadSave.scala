package com.spark.rdd.data

import org.apache.spark.SparkContext
import org.mortbay.util.ajax.JSON

/**
  * @fileName: RDDFileReadSave.java
  * @description: RDDFileReadSave.java类说明
  * @author: by echo huang
  * @date: 2020-06-27 23:39
  */
object RDDFileReadSave extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "fileReadSave")
    jsonFile(sc)
  }

  /**
    * json
    *
    * @param sc
    */
  def jsonFile(sc: SparkContext) = {
    val jsonFile = sc.textFile("logs/test.json")
    val jsonValue = jsonFile.map(JSON.parse)
    jsonValue.collect()
      .foreach(println)
  }
}
