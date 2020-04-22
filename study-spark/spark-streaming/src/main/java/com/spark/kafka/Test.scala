package com.spark.kafka

import com.alibaba.fastjson.JSON

import scala.collection.mutable

/**
  * @fileName: Test.java
  * @description: Test.java类说明
  * @author: by echo huang
  * @date: 2020-04-21 15:40
  */
object Test {

  def main(args: Array[String]): Unit = {

  }

  def getJson(json: String, key: String) {
    val jsonMap = JSON.parseObject(json, mutable.HashMap.getClass)

  }
}
