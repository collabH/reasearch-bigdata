package com.research.function

import com.research.objectoriented.Dog

/**
  * @fileName: MethodFunction.java
  * @description: MethodFunction.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 11:56
  */
object MethodFunction {
  def add(x: Int, y: Int): Int = {
    x + y
  }

  def three: Int = 1 + 1

  //没有返回值
  def sayHello(): Unit = {
    println("Hello World")
  }

  def main(args: Array[String]): Unit = {
    println(add(10, 20))
    println(three) //没有入参的函数括号可以不写
    sayHello()
    val dog = new Dog("s","s","ds")
  }

}
