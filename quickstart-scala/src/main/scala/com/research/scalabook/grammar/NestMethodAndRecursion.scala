package com.research.scalabook.grammar

import org.apache.commons.lang3.StringUtils

import scala.annotation.tailrec

/**
  * @fileName: NestMethodAndRecursion.java
  * @description: 嵌套方法的定义与递归
  * @author: by echo huang
  * @date: 2020-03-13 17:36
  */
object NestMethodAndRecursion extends App {

  def factorial(i: Int): Long = {
    @tailrec
    def fact(i: Int, acc: Int): Long = {
      if (i <= 1) acc
      else fact(i - 1, i * acc)
    }

    fact(i, 1)
  }

  0 to 5 foreach (i => println(factorial(i)))

  println(StringUtils.isBlank("ss"))

  val map = Map("string" -> "hello",
    "zhangsan" -> "lisl")

  println(map("string"))
}
