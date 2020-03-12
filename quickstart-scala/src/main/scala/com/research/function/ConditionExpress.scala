package com.research.function

/**
  * @fileName: ConditionExpress.java
  * @description: ConditionExpress.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 14:46
  */
object ConditionExpress {

  def condition(x: Int) = {

    if (x > 10) {
      x
    } else {
      x + 10
    }
  }

  def main(args: Array[String]): Unit = {
    println(condition(10))
  }
}
