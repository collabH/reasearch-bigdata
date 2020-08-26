package com.research.start.base

/**
  * @fileName: Ban.java
  * @description: Ban.java类说明
  * @author: by echo huang
  * @date: 2020-08-26 23:55
  */
class Ban private {
  print("xxx", this)

  override def toString: String = "hhh"
}

object Ban {

  def main(args: Array[String]): Unit = {
    println(new Ban)
  }
}
