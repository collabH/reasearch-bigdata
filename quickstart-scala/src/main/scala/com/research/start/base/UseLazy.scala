package com.research.start.base

/**
  * @fileName: UseLazy.java
  * @description: lazy应用
  * @author: by echo huang
  * @date: 2020-03-12 11:00
  */
class UseLazy {
  def main(args: Array[String]): Unit = {
    //第一次使用的时候才会去进行实例化
    lazy val a=10
  }
}
