package com.research.start.function

/**
  * @fileName: ChangeableParam.java
  * @description: 使用可变参数
  * @author: by echo huang
  * @date: 2020-03-12 12:25
  */
object ChangeableParam {
  def sum(a: Int, b: Int) = a + b

  //可变参数
  def sum2(a: Int*) = {
    var sum = 0
    for (elem <- a) {
      sum += elem
    }
    sum
  }


  def main(args: Array[String]): Unit = {
    println(sum(1,4))

    println(sum2(1,2,34,5,6,7,8))
  }
}
