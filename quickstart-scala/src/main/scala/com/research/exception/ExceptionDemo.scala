package com.research.exception

/**
 * @fileName: ExceptionDemo.scala
 * @description: ExceptionDemo.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 11:51 下午
 */
class ExceptionDemo {

  def main(args: Array[String]): Unit = {
    try {
      Thread.sleep(1000)
    } catch {
      case interruptedException: InterruptedException =>
        println("xxx")
    }
  }
}
