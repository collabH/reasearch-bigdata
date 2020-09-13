package com.research.scalabook

import java.util.Date

/**
 * @fileName: FunctionAndClosures.scala
 * @description: FunctionAndClosures.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 3:24 下午
 */
// executor around method
class Resource private() {
  println("Starting transaction...")

  private def cleanUp() {
    println("Ending transaction...")
  }

  def op1 = println("Operation 1")

  def op2 = println("Operation 2")

  def op3 = println("Operation 3")
}

object Resource {
  def use(codeBlock: Resource => Unit) {
    val resource = new Resource

    try {
      codeBlock(resource)
    }
    finally {
      resource.cleanUp()
    }
  }

  def main(args: Array[String]): Unit = {
    use((data: Resource) => {
      data.op1
      data.op2
      data.op3
    })
  }
}

// 偏函数
class PartialFunction {

  def log(date: Date, message: String) = {
    println(date, message)
  }
}

object App {
  def main(args: Array[String]): Unit = {
    val function = new PartialFunction

    // 偏函数
    val partialFunction: String => Unit = function.log(new Date(), _: String)

    partialFunction("大")
    partialFunction("小")
  }
}