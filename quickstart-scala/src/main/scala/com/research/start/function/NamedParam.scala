package com.research.start.function

/**
  * @fileName: NamedParam.java
  * @description: 命名参数学习
  * @author: by echo huang
  * @date: 2020-03-12 12:20
  */
object NamedParam {

  def speed(distance: Float, time: Float) = {
    distance / time
  }

  def main(args: Array[String]): Unit = {
    println(speed(100,10))
    println(speed(distance = 100,time = 10))
    println(speed(time = 100, distance= 1011))
  }
}
