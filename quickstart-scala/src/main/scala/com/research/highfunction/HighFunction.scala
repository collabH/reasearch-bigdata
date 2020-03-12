package com.research.highfunction

/**
  * @fileName: HighFunction.java
  * @description: scala高阶函数
  * @author: by echo huang
  * @date: 2020-03-13 00:21
  */
object HighFunction extends App {
  val sumList = List(1, 2, 3, 4, 5, 6, 7, 8)

  //map:逐个区操作集合中的每一个元素
  //  list.map(k => k + 1)
  sumList.map(_ + 1)

  //filter:对元素进行过滤
  sumList.map(_ * 2)
    .filter(_ > 8).foreach(println)

  //reduce: 第一个元素和第二个元素操作
  sumList.reduce(_ / _)
  sumList.reduce((a, b) => a - b)

  println(sumList.reduceLeft(_ - _))

  println(sumList.reduceRight(_ - _))

  //option对象防止非空
  println(sumList.reduceOption(_ - _))

  println(sumList.fold(0)(_ - _))

  println(sumList.foldLeft(0)(_ - _))

  println(sumList.foldRight(0)(_ - _))

  //flatMap:将数据扁平化 ==map+flatten
  val list = List(List(1, 2), List(3, 4), List(5, 6), List(7, 8))

  print(list.flatten)
  println()
  //将list中的每一个元素乘2
  print(list.map(_.map(_ * 2)))
  println()
  print(list.flatMap(_.map(_ * 2)))
  println("-------------")


  //读文件
  val fileSource: String = scala.io.Source.fromFile(HighFunction.getClass.getClassLoader.getResource("word.txt").getPath).mkString
  val sourceList = List(fileSource)
  sourceList.flatMap(_.split(","))
    .map((_, 1))
    .foreach(println)

}
