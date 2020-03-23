package com.research.start.function

/**
  * @fileName: CirculateExpression.java
  * @description: 循环表达式
  * @author: by echo huang
  * @date: 2020-03-12 14:48
  */
object CirculateExpression {

  def toExpress() = {
    1 to 10
    1.to(10)
  }

  def rangeExpress() = {
    Range(1, 10, 2)
  }

  def untilExpress() = {
    1.until(10)
    1 until 10
  }

  def toFor() = {
    for (i <- 1.to(10)) {
      println(i)
    }
  }

  /**
    * for循环打印1-9的偶数
    */
  def untilFor() = {
    for (i <- 1.until(10) if i % 2 == 0) println(i)
  }

  def arrayFor() = {
    val arr = Array("Hadoop", "Hadoop", "Hadoop", "Hadoop")
    for (elem <- arr) {
      println(elem)
    }

    arr.foreach(f => println(f))


    //循环判断数组内部数据 是否全部为xxx
    println(arr.forall(f => f.equals("Hadoop")))


    var (num, sum) = (100, 0)
    while (num > 0) {
      sum += num
      num -= 1
    }
    println(sum)
  }

  def main(args: Array[String]): Unit = {
    toFor()
    untilFor()
    arrayFor()
  }
}
