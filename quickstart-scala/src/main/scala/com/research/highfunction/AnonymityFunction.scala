package com.research.highfunction

/**
  * @fileName: AnonymityFunction.java
  * @description: 匿名函数
  * @author: by echo huang
  * @date: 2020-03-13 00:13
  */
object AnonymityFunction extends App {

  def sayHello(name: String) = {
    println(s"sayHello:$name")
  }

  /**
    * 匿名函数：函数是可以命名也可以不命名
    * (参数名:参数类型) =>函数体
    */
  //匿名函数传递给变量
  val function = (x: Int) => x + 1

  //调用
  println(function.apply(10))
  println(function(11))


  //匿名函数传递给函数
  def add = (x: Int, y: Int) => x + y

  println(add.apply(1, 2))

}
