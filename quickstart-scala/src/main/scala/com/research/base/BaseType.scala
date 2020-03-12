package com.research.base

/**
  * @fileName: BaseType.java
  * @description: BaseType.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 10:56
  */
class BaseType {

  def main(args: Array[String]): Unit = {
    val c = false

    val b: Byte = 1

    val s: Short = 2

    val d: Double = 20.0

    val f = 10.0f

    val name = "10"

    //类型转换
    val i = name.asInstanceOf[Int]

  }

}
