package com.research.collection

import scala.collection.mutable

/**
  * @fileName: MapApp.java
  * @description: MapApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 18:41
  */
object MapApp extends App {

  private val map: mutable.HashMap[String, String] = scala.collection.mutable.HashMap[String, String]()


  map.put("hello", "最近啊美好")

  private val iterator: Iterator[(String, String)] = map.iterator
  while (iterator.hasNext) {
    println(iterator.next())
  }

}
