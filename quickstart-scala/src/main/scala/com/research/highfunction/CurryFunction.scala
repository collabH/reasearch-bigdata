package com.research.highfunction

/**
  * @fileName: CurryFunction.java
  * @description: CurryFunction.java类说明
  * @author: by echo huang
  * @date: 2020-03-13 00:19
  */
object CurryFunction extends App {

  def sum(a: Int, b: Int) = a + b

  println(sum(2, 3))

  //curry函数 spark sql dataflow中使用，将原来接受俩个参数的函数转换为2
  def sum2(a: Int)(b: Int) = a + b

  println(sum2(2)(3))
}
