package com.research.collection

import scala.collection.mutable

/**
  * @fileName: SetApp.java
  * @description: Set学习
  * @author: by echo huang
  * @date: 2020-03-12 18:18
  */
object SetApp extends App {

  //不可变，声明后的集合就无法改变了
  val set: Set[Int] = Set(1, 2, 3, 4, 5)
  println(set.+(10))
  println(set)

  //可变Set集合
  private val set1: mutable.Set[Int] = scala.collection.mutable.Set(12, 3, 4, 5, 6)
  set1 += 1
  set1 += 2
  set1.head
}
