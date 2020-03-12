package com.research.collection

import scala.collection.mutable.ArrayBuffer

/**
  * @fileName: ArrayApp.java
  * @description: 继承App延迟加载
  * @author: by echo huang
  * @date: 2020-03-12 17:24
  */
object ArrayApp extends App {

  //数组操作
  //第一种方式
  val arr = new Array[String](5)

  arr(1) = "张三"
  println(arr(1))

  //第二种方法,通过Apply来实现
  val arr1 = Array("Hello", "World")

  val c = Array(1, 2, 3, 4, 5, 6)
  println(c.sum)
  println(c.max)
  //根据分隔符转换
  println(c.mkString("{", "", "}"))

  //可变数据
  val mArr: ArrayBuffer[Int] = ArrayBuffer[Int]()

  val ints: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8)
  ints.foreach(k => print(k))


  println()
  mArr += 1
  mArr += 2
  mArr += (3, 4, 5, 6)
  //添加数组
  mArr ++= Array(7, 8, 9, 0)
  //指定位置添加
  mArr.insert(1, 100)

  //删除元素
  mArr.remove(1)
  //遍历
  mArr.result().foreach(k => println(k))
  println(mArr.sum)
  //输出元素
  println(mArr.trimEnd(0))

  //转换为不可变
  mArr.toArray

  mArr.reverse.foreach(k => print(k))

  println()
  println(mArr)

}
