package com.research.start.objectoriented

/**
  * @fileName: CaseClassApp.java
  * @description: CaseClassApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 17:13
  */
//通常用在模式匹配
object CaseClassApp {

  def main(args: Array[String]): Unit = {
    println(Dog1("张三").name)

    println(Dog1.apply("狗"))
  }
}

case class Dog1(name: String) {
}
