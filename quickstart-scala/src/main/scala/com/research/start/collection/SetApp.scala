package com.research.start.collection

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

  set filter {
    case data =>
      data > 4
  } foreach (println)

  //可变Set集合
  private val set1: mutable.Set[Int] = scala.collection.mutable.Set(12, 3, 4, 5, 6)
  // 添加元素
  set1 += 1
  set1 += 2
  set1.head

  // 合并俩个set
  private val mergeSet: Set[Int] = set ++ set1

  println(mergeSet)

  // map操作
  mergeSet.map((data: Int) => data * 2)
    .foreach(println)

  // &  计算交集
  println(set & set1)

  // &~ 计算差集
  println(set &~ set1)

}
