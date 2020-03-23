package com.research.start.objectoriented

/**
  * @fileName: AbstractApp.java
  * @description: 抽象类入口
  * @author: by echo huang
  * @date: 2020-03-12 16:45
  */
object AbstractApp {

  def main(args: Array[String]): Unit = {
    val student = new Student
    student.sex = "男"
    student.name = "🐲"

    println(student.speak("哈哈哈"))
    student.eat
    println(student.watch("哦哦哦"))
  }
}

abstract class AbstractPerson {

  var name: String = _
  var sex: String = _

  def eat

  def watch(undefined: String): String

  def speak(undefined: String): String
}

class Student extends AbstractPerson {

  override def eat: Unit = println(this.name, this.sex, "吃东西了")

  override def watch(undefined: String): String = undefined

  override def speak(undefined: String): String = undefined
}
