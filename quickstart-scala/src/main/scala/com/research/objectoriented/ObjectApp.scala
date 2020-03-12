package com.research.objectoriented

/**
  * @fileName: ObjectApp.java
  * @description: 面向对象入口
  * @author: by echo huang
  * @date: 2020-03-12 15:28
  */
object ObjectApp {


  def main(args: Array[String]): Unit = {
    val person = new ScalaPeople()

    person.name = "longfuyou"
    //    person.age = 10 常量无法重新赋值报错
    println(person.name + ":" + person.age)
    println(person.eat())
    println(person.watch("av"))

    person.printInfo()
    println(person.sex)

  }
}

class ScalaPeople {
  //var变量会生成getter/setter
  //_代表占位符
  var name: String = _
  //常量只会生成getter
  val age = 10

  //private[this]修饰代表该类私有 [objectoriented]代表objectoriented包下都可以访问
  private[objectoriented] val sex = "male"

  def printInfo() = {
    println("sex:" + sex)
  }

  /**
    * 吃
    *
    * @return
    */
  def eat() = {
    this.name + ":eat...."
  }

  /**
    * 看
    *
    * @param teamName
    * @return
    */
  def watch(teamName: String = "football") = {
    val builder = new StringBuilder()
    builder.append(this.name)
      .append(":watch:")
      .append(teamName)
      .toString()
  }
}
