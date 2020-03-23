package com.research.start.function

/**
  * @fileName: DefaultParam.java
  * @description: 默认参数使用
  * @author: by echo huang
  * @date: 2020-03-12 12:07
  */
object DefaultParam {
  def sayName(name: String = "fuyou loong"): Unit = {
    println(name)
  }

  def main(args: Array[String]): Unit = {
    sayName("hsm")
    sayName()
  }
}
