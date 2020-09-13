package com.research.start.collection

/**
 * @fileName: MapApp.java
 * @description: MapApp.java类说明
 * @author: by echo huang
 * @date: 2020-03-12 18:41
 */
object MapApp extends App {
  // create
  val map = Map("hsm" -> 24, "wy" -> 24)
  println(map)

  // filter
  map filter { case (name: String, age: Int) =>
    println(s"$name --- $age")
    "hsm".equals(name) && 24 == age
  } foreach (println)

  // get
  println(map("hsm"))
  println(map.get("wy"))
  println(map get "wy")

  // put 不可变
  println(map.+("zzl" -> 24))
  println(map.+(("ls", 11)))
  println(map.+("hsm1"))

  // update 向不变容器添加元素
  private val stringToInt: Map[String, Int] = map.updated("wy1", 24)

  println(map)
}
